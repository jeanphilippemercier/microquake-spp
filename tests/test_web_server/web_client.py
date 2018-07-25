import obspy
import urllib.request as urllib_request
import io

#url='http://40.76.192.141:5000/getStream?starttime=2018-05-23T11:00:18.000&endtime=2018-05-23T11:00:19.00'


timeout=420
headers = {}
handlers = []
url_opener = None

url_base = 'http://40.76.192.141:5000/getStream'
def get_url(starttime=None, endtime=None):
    start_time = str(starttime)
    end_time   = str(endtime)
    url = '%s?starttime=%s&endtime=%s' % (url_base, start_time, end_time)
    return(url)


def set_opener(user, password):
    # Only add the authentication handler if required.
    handlers = []
    if user is not None and password is not None:
        # Create an OpenerDirector for HTTP Digest Authentication
        password_mgr = urllib_request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, self.base_url, user, password)
        handlers.append(urllib_request.HTTPDigestAuthHandler(password_mgr))

    if (user is None and password is None) or self._force_redirect is True:
        # Redirect if no credentials are given or the force_redirect
        # flag is True.
        #handlers.append(CustomRedirectHandler())
        pass
    else:
        handlers.append(NoRedirectionHandler())

    # Don't install globally to not mess with other codes.
    url_opener = urllib_request.build_opener(*handlers)
    print('Installed new opener with handlers: {!s}'.format(handlers))
    return url_opener

def get_stream(url):
    request = urllib_request.Request(url=url, headers=headers)
    #url_opener=set_opener(None, None)
    url_opener = urllib_request.build_opener()
    url_obj = url_opener.open(request, timeout=timeout)
    data_stream = io.BytesIO(url_obj.read())

    data_stream.seek(0, 0)
    st = obspy.read(data_stream, format="MSEED")
    data_stream.close()
    return(st)

def get_stream_from_mongo(starttime, endtime):
#starttime='2018-05-23T10:51:02.000'
#endtime  ='2018-05-23T10:51:03.000'
#starttime='2018-05-23T10:51:03.7'
#endtime  ='2018-05-23T10:51:04.7'
    

    url = get_url(starttime, endtime)
    st = get_stream(url)
    return(st)

'''
for tr in st:
    print('%s: %s - %s' % (tr.get_id(), tr.stats.starttime, tr.stats.endtime))
    #tr.plot()
#data_stream.close()
print ('stream has %d traces' % len(st))
'''
