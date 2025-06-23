import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import time


playlists = pd.DataFrame(columns=['playlist_name', 'playlist_id', 'followers'])
artists = pd.DataFrame(columns=['artist_id', 'artist_name'])
tracks = pd.DataFrame(columns=['track_name', 'track_id'])
playlists_tracks = pd.DataFrame(columns=['playlist_id', 'track_id'])
tracks_artists = pd.DataFrame(columns=['track_id', 'artist_id'])
curators_playlists = pd.DataFrame(columns=['curator', 'playlist_id'])


def fetch_spotify_playlists(client_id, client_secret, limit=100, startfrom=None):
    """
    Fetch playlists from Spotify for a list of curators and store them in CSV files.
    
    :param client_id: Spotify API client ID.
    :param client_secret: Spotify API client secret.
    :param limit: Number of playlists to fetch per request (default is 100).
    :param startfrom: Curator username to start fetching from (default is None, which
                     means start from the beginning).
    """
    global playlists, artists, tracks, playlists_tracks, tracks_artists, curators_playlists
    
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=client_id,
        client_secret=client_secret
    ))
    
    # Get the list of curators
    curators = pd.read_csv('dataset/curators.csv')['username']
    
    if startfrom is not None:
        # Filter curators starting from a specific index
        index = curators[curators == startfrom].index[0]
        curators = curators[index:]
        # initialize dataframes from existing CSV files
        playlists = pd.read_csv('dataset/playlists.csv')
        artists = pd.read_csv('dataset/artists.csv')
        tracks = pd.read_csv('dataset/tracks.csv')
        playlists_tracks = pd.read_csv('dataset/playlists_tracks.csv')
        tracks_artists = pd.read_csv('dataset/tracks_artists.csv')
        curators_playlists = pd.read_csv('dataset/curators_playlists.csv')

    i = 0
    for curator in curators:
        print(f"Fetching playlists for curator: {curator} ({i+1}/{len(curators)})")
        i += 1
        offset = 0
        while True:
            response = safe_request(sp.user_playlists, user=curator, limit=limit, offset=offset)
            if response == {}:
                break
            items = response.get('items', [])
            if not items:
                break

            j = 0

            for playlist in items:
                j += 1
                # Add curator-playlist association
                curators_playlists = pd.concat(
                    [curators_playlists, pd.DataFrame(
                    [{'curator': curator, 'playlist_id': playlist['id']}])],
                    ignore_index=True
                )

                if playlist['id'] not in playlists['playlist_id'].values:
                    try:
                        print(f"\tProcessing playlist: {playlist['name']} [{j}/{len(items)}]")
                        details = safe_request(
                            sp.playlist, playlist['id'], 
                            fields="followers.total"
                        )
                        if details == {}:
                            continue

                        # Add playlist
                        playlists = pd.concat([
                            playlists,
                            pd.DataFrame(
                                [{
                                    'playlist_name': playlist['name'],
                                    'playlist_id': playlist['id'],
                                    'followers': details['followers']['total']
                                }]
                            )],
                            ignore_index=True
                        )
                        
                        handle_new_playlist(playlist, sp)

                    except spotipy.exceptions.SpotifyException as e:
                        print(e)
                        continue

            offset += limit
            
        # Save the dataframes to CSV files after processing each curator
        playlists.to_csv('dataset/playlists.csv', index=False)
        artists.to_csv('dataset/artists.csv', index=False)
        tracks.to_csv('dataset/tracks.csv', index=False)
        playlists_tracks.to_csv('dataset/playlists_tracks.csv', index=False)
        tracks_artists.to_csv('dataset/tracks_artists.csv', index=False)
        curators_playlists.to_csv('dataset/curators_playlists.csv', index=False)


def handle_new_playlist(playlist, sp):
    """
    Handle a new playlist by adding it to the playlists DataFrame.
    
    :param playlist: Dictionary containing playlist details.
    :param sp: An authenticated Spotipy client instance for interacting with the Spotify API.
    """
    global artists, tracks, playlists_tracks, tracks_artists
    
    # Retrieve all tracks in the playlist
    tracks_response = safe_request(sp.playlist_tracks, playlist['id'])
    if tracks_response == {}:
        return
    
    for item in tracks_response.get('items', []):
        track = item.get('track', {})
        
        if track and track.get('id') and track.get('name'):
            # Add playlist-track association
            playlists_tracks = pd.concat(
                [
                    playlists_tracks,
                    pd.DataFrame([
                        {
                            'playlist_id': playlist['id'], 'track_id': track.get('id')
                        }
                    ])
                ], ignore_index=True)
            
            if track['id'] not in tracks['track_id'].values:
                tracks = pd.concat(
                    [
                        tracks, pd.DataFrame(
                            [{'track_name': track['name'], 'track_id': track['id']}])
                    ], ignore_index=True
                )
                handle_new_track(track)
                


def handle_new_track(track):
    """
    Handles the processing of a new track using the provided Spotify client.
    
    :param track: A dictionary containing information about the track to be processed.
    """
    global artists, tracks_artists

    for artist in track.get('artists', []):
        if artist.get('id') and artist.get('name'):
            # Add artist if not already present
            if artist['id'] not in artists['artist_id'].values:
                artists = pd.concat([
                    artists,
                    pd.DataFrame([{'artist_id': artist['id'], 'artist_name': artist['name']}])
                ], ignore_index=True)
            # Add track-artist association
            tracks_artists = pd.concat([
                tracks_artists,
                pd.DataFrame([{'track_id': track['id'], 'artist_id': artist['id']}])
            ], ignore_index=True)
    

def safe_request(func, *args, **kwargs):
    """
    Safely make a request to the Spotify API, handling rate limits and errors.
    
    :param func: The function to call (e.g., sp.user_playlists, sp.playlist).
    :param args: Positional arguments for the function.
    :param kwargs: Keyword arguments for the function.
    :return: The response from the Spotify API or an empty dictionary if an error occurs.
    """
    while True:
        try:
            return func(*args, **kwargs)
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", 5))
                print(f"Rate limited. Retrying after {retry_after} seconds.\n[{e}]")
                time.sleep(retry_after)
            elif e.http_status == 404:
                print(f"Resource not found: {e}")
                return {}
            else:
                raise