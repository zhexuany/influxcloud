package control

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	DefaultBindAddr     = "localhost:8091"
	DefaultMaxRetryTime = 5
)

type Client struct {
	logger   log.Logger
	BindAddr string
}

func NewClient(addr string) *Client {
	if addr == "" {
		addr = DefaultBindAddr
	}

	return &Client{
		BindAddr: addr,
	}
}

func (c *Client) retryWithBackoff(fn func() error) error {
	counter := 0
	for err := fn(); err != nil; counter++ {
		c.logger.Printf("Retrying after %ds", counter)
		time.Sleep(time.Duration(counter) * time.Second)
		if counter == DefaultMaxRetryTime {
			return errors.New("operation time out")
		}
	}
	return nil
}

func (c *Client) do(method string, url string) (*http.Response, error) {
	// url.Values.Encode()
	// url.URL.RequestURI()
	// url.URL.String()
	req, err := http.NewRequest(method, url, nil)
	// if err != nil {
	// 	return err
	// }

	// req.Header.Add("", "")
	// req.Header.Set("", "")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	// resp.Header.Get("")
	return resp, nil
}

func (c *Client) get(url string) (*http.Response, error) {
	return c.do("GET", url)
}

func (c *Client) post(url string) (*http.Response, error) {
	return c.do("POST", url)
}

func (c *Client) postForm(url string, data url.Values) (*http.Response, error) {
	// url.Values.Encode()
	// http.DefaultClient.PostForm(url string, data url.Values)
	// c.do("POST")
	return nil, nil
}

func errorFromResponse(resp *http.Response) error {
	// json.NewDecoder(resp.Body).Decode()
	return nil
}

// AddDataNode will add tcpAddr as a data node into cluster
func (c *Client) AddDataNode(tcpAddr string) error {
	if tcpAddr == "" {
		return errors.New("tcpAddr is empty")
	}

	fn := func() error {
		resp, err := c.postForm(c.BindAddr+"/add-data", nil)
		if err != nil {
			return err
		}

		if resp.StatusCode == 200 {
			// json.NewDecoder(resp.Body).Decode(v)
			return nil
		} else {
			err := errorFromResponse(resp)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return c.retryWithBackoff(fn)
}

// AddMetaNode will add tcpAddr as a meta node into cluster
func (c *Client) AddMetaNode(tcpAddr string) error {
	if tcpAddr == "" {
		return errors.New("tcpAddr is empty")
	}

	//create a object
	fn := func() error {
		resp, err := c.postForm(c.BindAddr+"/add-meta", nil)
		if err != nil {
			return err
		}

		if resp.StatusCode == 200 {
			// json.NewDecoder(resp.Body).Decode(v)
			return nil
		} else {
			err := errorFromResponse(resp)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return c.retryWithBackoff(fn)
}

// LeaveCluster removes potential meta and data node from localhost
func (c *Client) LeaveCluster() {

}

func (c *Client) Lease() {

}

// Ping will ping cluster's element in order to make sure each node is still
// alive
func (c *Client) Ping() {

}

// removeNodeByAddress will remove a node accoridng to addr
func (c *Client) removeNodeByAddress(addr string) {

}

// RemoveDataNode will remove a data node according to addr
func (c *Client) RemoveDataNode(addr string) {

}

// RemoveMetaNode will remove a meta node according to addr
func (c *Client) RemoveMetaNode(addr string) {

}

// ShowCluster will show statistics of cluster
func (c *Client) ShowCluster() {

}

func (c *Client) Status() {

}

func (c *Client) DataNodeStatus() {

}

func (c *Client) Token() {

}

// UpdateDataNode will update data node's addr from oldv to newV
func (c *Client) UpdateDataNode(oldV, newV string) {

}

func (c *Client) CopyShard(src, dst string, id uint64) error {

	fn := func() error {
		shardID := strconv.FormatUint(id, 64)
		var data url.Values
		data = make(map[string][]string)
		data["src"] = []string{src}
		data["dst"] = []string{dst}
		data["shardID"] = []string{shardID}
		_, err := c.postForm(c.BindAddr+"copy-shard", data)
		if err != nil {
			return err
		}
		return nil
	}

	return c.retryWithBackoff(fn)
}

func (c *Client) CopyShardStatus() {

}

func (c *Client) ShowShards() {

}

func (c *Client) KillCopyShard() {

}

func (c *Client) RemoveShard() {

}

func (c *Client) TruncateShards() {

}

func (c *Client) BackupShard() {

}

func (c *Client) RestoreShard() {

}

func (c *Client) dialRPC() {

}

func (c *Client) GetSnapshot() {

}

func (c *Client) RestoreMeta() {

}

func (c *Client) Users() {

}
