# canadian-historical-weather-radar
Downloads historical GIF images for Canadian weather radar in bulk.

Downloads one image per hour between the specified start and end date times. The user must specify which site or site aggregate to pull from, and
what image type they are interested in. 

This is an example set of command line arguments for Atlantic Canada:

```
canadian-historical-weather-radar.exe --directory bla --end-day 5 --end-month 2 --end-year 2021 --image-type PRECIPET_RAIN_WEATHEROFFICE --site ATL --start-day 1 --start-month 1 --start-year 2007
```

The Environment and Climate Change Canada servers respond quite slowly, so unfortunately these requests take a great deal of time to complete.

In case you're interested in a sense of perspective, all of the rain and snow images for Atlantic Canada between 2007-01 and 2021-02 have a total size
of approximately 2 GB on disk.
