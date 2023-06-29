# YouTubeChannelDataHarvesting1.0

YouTube Channels Data Harveting App

This App has been developed to Fetch data from the YouTube Channels Available on the Internet using an API connection(API:YouTube's Data API v3) . 
Has been developed for the users of the App to know the information related to any specific channel that one would like to know about.
Like for example: If one wants to know how the count of the suscribers to the channels, 
The videos that are part of the channel, the view count , likes, dislikes, comments etc.


User Guide:

You will have to Enter a Valid YouTube Channel name.
Then you have to Click on a Button to Extract data(FETCH DATA AND PUSH to MongoDB).
Once the Data is Extracted you will be able to see some basic information regrading the Channel Name that you had entered.

Section: Details of the Videos!
Drop Downs will be populated with the information extracted , and you cans select the "Channel Name", "Playlists", "Videos from the Playlist"
As per the selection in the Drop Downs in this Section of the APP, Details will be populated for your Perusal.

Section: Data Migration Zone
Drop Down For The Available Channels:
You need to Select the Channel Name from the Drop Down, Then click on the Button("Migrate to Mysql") for the Data to be populated into the Mysql Database.

Section: Channel Data Analysis Zone:
Firstly You will Have a Table on Display with all the available Channel Names for which data is Available for Analysis .

Section: Channel Analysis:
In this Section of the App, We have Pre-Populated Questions(10-Count) Which are most Commonly queried on The You Tube Channels Data.
These Questions are available in a SelectBox or a Dropbox, As you select any of the question related data shall be Displayed Along with a Visualization for Better user Experience.

Section : Vertical Tab on the Left : YouTube Channel Analysis!
Select Channel Name:
Select The kind of Chart that you would like to view the Information , as of now I have included 4  Type of Charts.
As you select all the required info The charts will be Populated in the Main Section and not in the vertical tab.

Note: This App has been Developed using The Open Source Platform Streamlit (For deployment as well as some of the UI Set up)

