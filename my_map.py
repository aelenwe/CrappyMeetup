from __future__ import division
import folium
from folium import features, plugins


def draw_map(poop, data, maplat=37.7749, maplon=-122.4194):
    """
    Create a map of meetup locations with a heatmap for reports of human waste

    :param poop: list of tuples for each meetup - results of fetch_poop()
    :param data: list of list of lat,lon for each poop
    :param maplat: starting point for map - default to San Francisco
    :param maplon: starting point for map - default to San Francisco
    :return: map object
    """
    # Poop emoji - not used at this time

    # Custom markers for the meetups
    meetup_icon = 'http://photos2.meetupstatic.com/photos/event/4/2/6/highres_147421062.jpeg'

    # Create map object
    mapa = folium.Map(location=[maplat, maplon], tiles="Cartodb Positron",
                      zoom_start=14)

    # Create and place marker with pop-up for each meetup
    for item in poop:
        loc, html, poop_count = item
        # Current meetup lat and lon
        lat, lon = item[0]
        icon = folium.features.CustomIcon(meetup_icon, icon_size=(20, 20))
        # Create the frame for the pop-up
        iframe = folium.element.IFrame(html=html, width=500, height=155)
        # Create the marker
        meetup_cood = features.Marker([lat, lon], icon=icon)
        # Create the pop-up
        name = features.Popup(iframe, max_width=2650)
        # Add the pop-up to the marker
        meetup_cood.add_children(name)
        # Add the marker to the map
        mapa.add_children(meetup_cood)
    # Create heatmap of poop reports with custom colormap
    hm = plugins.HeatMap(data, min_opacity=0, radius=15, blur=15,
                         gradient={0.4: '#cd8500', 0.65: '#9a6300', 1: '#674200'})
    # Add heatmat to the map
    mapa.add_children(hm)
    # Save map to outside file
    mapa.save('/Users/aelenwe/Lisa Dropbox/Dropbox/Public/crappy_meetup.html')
    return mapa
