# Dataset
<!-- https://www.kaggle.com/datasets/jacobbaruch/basketball-players-stats-per-season-49-leagues?resource=download -->
The dataset contains playlists, songs and artists.
These were fetched through Spotify's Web API.
Since Playlists created by Spotify return error code 404 (since 27 November, 2024: https://developer.spotify.com/blog/2024-11-27-changes-to-the-web-api), reliable playlist curators were used instead, mixing journalistic and non-journalistic curators.
<!-- test with both? -->

## Structure
### Main data
- *playlists.csv*: contains the playlists' names, IDs, number of followers
- *tracks.csv*: contains the tracks' names, IDs
- *curators.csv*: contains playlist curators' IDs, names, and type (journalist, creator...)
- *artists.csv*: contains artists IDs, names

### Associations
- *playlist_tracks.csv*: contains associations between playlists and tracks
- *tracks_artists.csv*: contains associations between tracks and artists
- *curators_playlists.csv*: contains associations between curators and playlists (required because of collaborative playlists)