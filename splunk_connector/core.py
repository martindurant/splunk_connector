
import base64
import io
import requests
import pandas as pd
import time


class SplunkConnect:

    """
    Talk to Splunk over REST, download data to dataframes

    If there is no pre-fetched auth key, must call ``.auth(user, pw)`` to
    establish credentials, or ``.auth_head(user=user, pw=pw)`` to use simple
    auth on all calls.

    Main user methods: read_pandas, read_pandas_iter, read_dask

    Parameters
    ----------
    base_url: str
        Address to contact Splunk on, e.g., ``https://localhost:8089``
    key: str
        Auth key, if known
    """

    POLL_TIME = 1  # seconds to sleep between successive polls
    TIMEOUT = 600  # maximum seconds to wait for query to finish

    def __init__(self, base_url, key=None):
        self.url = base_url
        self.key = key
        if key:
            self.auth_head(key)

    def auth(self, user, pw):
        """
        Login to splunk and get a session key
        """
        url = self.url + '/services/auth/login?output_mode=json'
        r = requests.post(url, verify=False, data={'username': user, 
                                               'password': pw})
        self.key = r.json()['sessionKey']
        self.auth_head(self.key)
    
    def auth_head(self, key=None, user=None, pw=None):
        """
        Make header either by session key or by user/pass
        """
        if key:
            self.head = {'Authorization': 'Splunk %s' % key}
        elif user is None and pw is None:
            raise ValueError('Must supply key or user/password')
        else:
            code = "%s:%s" % (user, pw)
            self.head = {'Authorization': 'Basic %s' % base64.b64encode(
                         code.encode()).decode()}
    
    @staticmethod
    def _sanitize_query(q):
        """
        Ensure that all queries are actually valid searches
        """
        q = q.strip()
        if not q.startswith('search') and not q.startswith('|'):
            return "search " + q
        return q
    
    def list_saved_searches(self):
        """
        Get saved search names/definitions as a dict
        """
        r = requests.get(self.url + '/services/saved/searches?output_mode=json',
                          headers=self.head, verify=False)
        out = r.json()['entry']
        return {o['name']:o['content']['search'] for o in out}
    
    def start_query(self, q):
        """
        Initiate a query as a job
        """
        q = self._sanitize_query(q)
        r = requests.post(self.url + '/services/search/jobs?output_mode=json',
                          verify=False, data={'search': q},
                          headers=self.head)
        return r.json()['sid']
    
    def poll_query(self, sid):
        """
        Check the status of a job
        """
        path =  '/services/search/jobs/{}?output_mode=json'.format(sid)
        r = requests.get(self.url +path,  verify=False, headers=self.head)
        out = r.json()['entry'][0]['content']
        return out['isDone'], out.get('eventCount', 0)

    def wait_poll(self, sid):
        time0 = time.time()
        while True:
            done, count = self.poll_query(sid)
            if done:
                return done, count
            if time.time() - time0 > self.TIMEOUT:
                raise RuntimeError("Timeout waiting for Splunk to finish query")
            time.sleep(self.POLL_TIME)

    def get_query_result(self, sid, offset=0, count=0):
        """
        Fetch query output (as CSV)
        """
        path = ('/services/search/jobs/{}/results/?output_mode=csv'
                '&offset={}&count={}').format(sid, offset, count)
        r = requests.get(self.url + path,  verify=False, headers=self.head)
        return r.content

    def get_dataframe(self, sid, offset=0, count=0, **kwargs):
        """
        Read a chunk from completed query, return a pandas dataframe

        Parameters
        ----------
        sid: str
            The job's ID
        offset: int
            Starting row
        count: int
            Number of rows to fetch
        kwargs: passed to pd.read_csv
        """
        txt = self.get_query_result(sid, offset, count)
        return pd.read_csv(io.BytesIO(txt), **kwargs)

    def read_pandas(self, q, **kwargs):
        """
        Start query, wait for completion and download data as a dataframe

        Parameters
        ----------
        q: str
            Valid Splunk query
        kwargs: passed to pd.read_csv
        """
        sid = self.start_query(q)
        self.wait_poll(sid)
        return self.get_dataframe(sid, **kwargs)

    def read_pandas_iter(self, q, chunksize, **kwargs):
        """
        Start query, wait for completion and make an iterator of dataframes

        Parameters
        ----------
        q: str
            Valid Splunk query
        chunksize: int
            Number of rows in each dataframe
        kwargs: passed to pd.read_csv
        """
        sid = self.start_query(q)
        done, count = self.wait_poll(sid)
        for i in range(0, count, chunksize):
            yield self.get_dataframe(sid, offset=i, count=chunksize, **kwargs)

    def read_dask(self, q, chunksize, **kwargs):
        """
        Start query, wait for completion, return lazy dask dataframe.

        This does download the first 20 rows in this thread, to infer dtypes.

        Parameters
        ----------
        q: str
            Valid Splunk query
        chunksize: int
            Number of rows in each dataframe
        kwargs: passed to pd.read_csv
        """
        from dask import delayed
        import dask.dataframe as dd
        sid = self.start_query(q)
        done, count = self.wait_poll(sid)
        meta = self.get_dataframe(sid, count=20)[:0]
        parts = [delayed(self.get_dataframe)(sid, offset=i, count=chunksize,
                                             **kwargs)
                 for i in range(0, count, chunksize)]
        return dd.from_delayed(parts, meta=meta)
