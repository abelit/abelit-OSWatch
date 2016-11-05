import sys
import weibo
import webbrowser
 
APP_KEY = ''
MY_APP_SECRET = ''
REDIRECT_URL = 'https://api.weibo.com/oauth2/default.html'

api = weibo.APIClient(app_key=APP_KEY,app_secret=MY_APP_SECRET,redirect_uri=REDIRECT_URL)
authorize_url = api.get_authorize_url()
print(authorize_url)
webbrowser.open_new(authorize_url)