/* custom adapter to register found instances with HTTP flag into local
hipache, and set/unset tags from docker bridge0 consul as necessary */

package hipache

import (
	"errors"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gliderlabs/registrator/bridge"
	consulapi "github.com/hashicorp/consul/api"
)

func init() {
	bridge.Register(new(Factory), "hipache")
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	var consuldc, domain string
	redispool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", uri.Host)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	testconn := redispool.Get()
	defer testconn.Close()
	_, err := testconn.Do("PING")
	if err != nil {
		log.Fatal("hipache: ", uri.Scheme, err)
	}

	// see if our uri has a path component. If so use that for the target domain.
	if len(uri.Path) < 2 {
		domain = "hipache.service"
	} else {
		domain = uri.Path[1:]
	}

	// find consul?
	config := consulapi.DefaultConfig()
	config.Address = "172.17.42.1:8500"
	consulclient, err := consulapi.NewClient(config)
	if err != nil {
		log.Print("hipache: consul setup error: ", err)
		log.Print("hipache: running with only hipache redis support.")
		return &HipacheAdapter{pool: redispool, domain: domain}
	} else {
		log.Print("hipache: consul at: ", consulclient)
		// try consul path on default docker bridge
		dcinfo, err := consulclient.Agent().Self()
		if err == nil {
			consuldc = dcinfo["Config"]["Datacenter"].(string)
			log.Print("hipache: running in consul dc:", consuldc)
		} else {
			log.Print("hipache: consul error attempting to find dc:", err)
			log.Print("hipache: running with only hipache redis support.")
			return &HipacheAdapter{pool: redispool, domain: domain}
		}
		return &HipacheAdapter{pool: redispool, domain: domain, consul: consulclient, consuldc: consuldc}
	}

}

type HipacheAdapter struct {
	pool     *redis.Pool
	domain   string
	consul   *consulapi.Client
	consuldc string
}

func (r *HipacheAdapter) Ping() error {
	conn := r.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PING")
	if err != nil {
		return err
	}
	return nil
}

//Locates the first instance of target in items and returns (index, true), or (len(d), false)
func locate(items []string, target string) (index int, found bool) {
	findlen := 0
	for findlen, x := range items {
		if x == target {
			return findlen, true
		}
	}
	return findlen, false
}

// Remove all occurences of target from items. Returns the modified view.
func excise(items []string, target string) []string {
	b := items[:0] // use an inplace modifier to detag -- works because go slices are memory views
	for _, x := range items {
		if x != target {
			b = append(b, x)
		}
	}
	return b
}

func (r *HipacheAdapter) serviceEndpoint(service *bridge.Service, omitdc bool) string {
	nameParts := []string{service.Name, r.domain}
	if r.consuldc != "" {
		if !omitdc {
			nameParts = append(nameParts, r.consuldc)
		}
		nameParts = append(nameParts, "consul")
	}
	return strings.Join(nameParts, ".")
}

func (r *HipacheAdapter) serviceTarget(service *bridge.Service) string {
	return "http://" + service.IP + ":" + strconv.Itoa(service.Port)
}

// register a hipache service
// the reg and dereg functions figure out the following:
// * short and long consul names
// * backend service/port, though we assume http
// * wether we need to make a new key or just append
// * wether we need to do consul tagging
// * when to untag and remove frontend keys
func (r *HipacheAdapter) doHipacheServiceRegister(service *bridge.Service, omitdc bool) error {
	conn := r.pool.Get()
	defer conn.Close()

	servicename := r.serviceEndpoint(service, omitdc)
	rediskey := "frontend:" + servicename
	target := r.serviceTarget(service)

	svlist, err := redis.Strings(conn.Do("LRANGE", rediskey, 0, -1))
	if err != nil {
		return err
	}
	if len(svlist) > 0 { // frontend exists
		_, found := locate(svlist, target)
		if !found { // backend does not exist
			_, err := conn.Do("RPUSH", rediskey, target)
			if err != nil {
				return err
			}
		}
	} else { // frontend key does not exist
		_, err = conn.Do("RPUSH", rediskey, service.Name, target)
		if err != nil {
			return err
		}
	}
	return nil
}

// deregister a hipache service and MAYBE untag consul
func (r *HipacheAdapter) doHipacheServiceDeRegister(service *bridge.Service, omitdc bool) error {
	//var ConsulName string
	conn := r.pool.Get()
	defer conn.Close()

	servicename := r.serviceEndpoint(service, omitdc)
	rediskey := "frontend:" + servicename
	target := r.serviceTarget(service)

	flen, err := redis.Int64(conn.Do("LLEN", rediskey))
	if err != nil {
		return err
	}
	if flen > 0 { // frontend exists
		res, err := redis.Int64(conn.Do("LREM", rediskey, 0, target))
		if err != nil {
			return err
		}
		if res < 1 {
			log.Print("hipache: that's odd -- the backend is already deregged: ", rediskey, target)
		}
		if !omitdc && r.consul != nil { // for ONE of the dereg runs, if we are now empty, detag.
			res, err := redis.Int64(conn.Do("LLEN", rediskey))
			if err != nil {
				return err
			}
			if res < 2 { // only the identifier is left, or something really killed the key
				log.Print("all entries for frontend removed, detagging", rediskey)
				err = r.doConsulUnTagging(service)
				if err != nil {
					return err
				}
			}
		}
	} else { // frontend key does not exist
		log.Print("hipache: that's strange -- you're asking us to dereg something already deregged:", rediskey)
		// detag just in case
		err = r.doConsulUnTagging(service)
		if err != nil {
			return err
		}
	}
	return nil
}

func doConsulAddService(agent *consulapi.Agent, ConsulName string, address string) (*consulapi.AgentService, error) {
	registration := new(consulapi.AgentServiceRegistration)
	registration.Name = ConsulName
	registration.Port = 80
	registration.Address = address
	err := agent.ServiceRegister(registration)
	if err != nil {
		return nil, err
	}
	svcs, err := agent.Services()
	if err != nil {
		return nil, err
	}
	svc, found := svcs[registration.Name]
	if !found {
		return nil, errors.New("Could not find service " + registration.Name + " after registration!")
	}
	return svc, nil
}

// the types are so close yet SO DIFFERENT
func updateConsulTags(agent *consulapi.Agent, svc *consulapi.AgentService) error {
	registration := new(consulapi.AgentServiceRegistration)
	registration.Name = svc.Service
	registration.ID = svc.ID
	registration.Tags = svc.Tags
	registration.Port = svc.Port
	registration.Address = svc.Address
	return agent.ServiceRegister(registration)
}

func (r *HipacheAdapter) doConsulTagging(service *bridge.Service) error {
	var svc *consulapi.AgentService
	mytag := service.Name
	ConsulName := strings.Replace(r.domain, ".service", "", -1)
	agent := r.consul.Agent()
	consulsvcs, err := agent.Services()
	if err != nil {
		return err
	}
	svc, found := consulsvcs[ConsulName]
	if !found {
		svc, err = doConsulAddService(agent, ConsulName, service.Origin.HostIP)
		if err != nil {
			return err
		}
	}

	_, found = locate(svc.Tags, mytag)
	if !found {
		svc.Tags = append(svc.Tags, mytag)
		return updateConsulTags(agent, svc)
	}
	return nil
}

func (r *HipacheAdapter) doConsulUnTagging(service *bridge.Service) error {
	var svc *consulapi.AgentService
	var found bool
	mytag := service.Name
	ConsulName := strings.Replace(r.domain, ".service", "", -1)
	agent := r.consul.Agent()
	consulsvcs, err := agent.Services()
	if err != nil {
		return err
	}
	svc, found = consulsvcs[ConsulName]
	if !found {
		log.Print("hipache: how strange -- the service you want me to detag does not exist: ", ConsulName)
		return nil
	}
	svc.Tags = excise(svc.Tags, mytag)
	err = updateConsulTags(agent, svc)
	return nil
}

func (r *HipacheAdapter) Register(service *bridge.Service) error {
	err := r.doHipacheServiceRegister(service, false) // register fullname
	if err != nil {
		log.Print("hipache: ERROR: ", err)
		return err
	}
	if r.consul != nil {
		err = r.doHipacheServiceRegister(service, true) // register short consul name
		if err != nil {
			log.Print("hipache: ERROR: ", err)
			return err
		}
		err = r.doConsulTagging(service)
		if err != nil {
			log.Print("hipache: ERROR: ", err)
			return err
		}
	}
	return nil
}

func (r *HipacheAdapter) Deregister(service *bridge.Service) error {
	err := r.doHipacheServiceDeRegister(service, false) // register fullname
	if err != nil {
		log.Print("hipache: ERROR: ", err)
		return err
	}
	if r.consul != nil {
		err := r.doHipacheServiceDeRegister(service, true) // register short consul name
		if err != nil {
			log.Print("hipache: ERROR: ", err)
			return err
		}
	}
	return nil
}

func (r *HipacheAdapter) Refresh(service *bridge.Service) error {
	return nil
}
