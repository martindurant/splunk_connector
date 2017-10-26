
import base64
import io
import requests


class SplunkConnect:
    
    def __init__(self, base_url, key=None):
        self.url = base_url
        self.key = key
        if key:
            self.auth_head(key)

    def auth(self, user, pw):
        """Login to splunk and get a session"""
        url = self.url + '/services/auth/login?output_mode=json'
        r = requests.post(url, verify=False, data={'username': user, 
                                               'password': pw})
        self.key = r.json()['sessionKey']
        self.auth_head(self.key)
    
    def auth_head(self, key=None, user=None, pw=None):
        """Make header either by session key or by user/pass"""
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
        q = q.strip()
        if not q.startswith('search') and not q.startswith('|'):
            return "search " + q
        return q
    
    def list_saved_searches(self):
        r = requests.get(self.url + '/services/saved/searches?output_mode=json',
                          headers=self.head, verify=False)
        out = r.json()['entry']
        return {o['name']:o['content']['search'] for o in out}
    
    def start_query(self, q):
        """Initiate a query as a job"""
        q = self._sanitize_query(q)
        r = requests.post(self.url + '/services/search/jobs?output_mode=json',
                          verify=False, data={'search': q},
                          headers=self.head)
        return r.json()['sid']
    
    def poll_query(self, sid):
        """Check the status of a job"""
        path =  '/services/search/jobs/{}?output_mode=json'.format(sid)
        r = requests.get(self.url +path,  verify=False, headers=self.head)
        out = r.json()['entry'][0]['content']
        return out['isDone'], out.get('eventCount', 0)
        
    def get_query_result(self, sid, offset=0, count=0):
        """Fetch query output (as CSV)"""
        path =  ('/services/search/jobs/{}/results/?output_mode=csv'
                 '&offset={}&count={}').format(sid, offset, count)
        r = requests.get(self.url + path,  verify=False, headers=self.head)
        return r.content

    def get_dataframe(self, sid, offset=0, count=0, **kwargs):
        import pandas as pd
        txt = self.get_query_result(sid, offset, count)
        return pd.read_csv(io.BytesIO(txt), **kwargs)
