import spotipy
from spotipy.oauth2 import SpotifyOAuth

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id="c1d8196c3b484e19be269e9c586a056f",
    client_secret="df44612fbead4ce18867ca02b06f364c",
    redirect_uri="http://127.0.0.1:8888/callback",
    scope="user-top-read user-read-private user-read-recently-played"
))

token_info = sp.auth_manager.get_cached_token()
if not token_info:
    auth_url = sp.auth_manager.get_authorize_url()
    print(f"\nOpen this URL in your browser:\n{auth_url}\n")
    response = input("Paste the full redirect URL here: ")
    code = sp.auth_manager.parse_response_code(response)
    token_info = sp.auth_manager.get_access_token(code)

print("\nYour refresh token:")
print(token_info['refresh_token'])
