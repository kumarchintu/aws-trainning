from pytube import YouTube 

#where to save 
SAVE_PATH = "D:/pyth/" #to_do 

#link of the video to be downloaded 
link=["https://www.youtube.com/watch?v=Rt5G5Gj7RP0&feature=youtu.be",
      "https://www.youtube.com/watch?v=edgZo2g-LTM",
      "https://www.youtube.com/watch?v=pcdpk3Yd1EA",
      "https://www.youtube.com/watch?v=MaI0_XdpdP8",
      "https://www.youtube.com/watch?v=SWXuXhZkNQc&t=110s"
      ]#list of youtube links which need to be downloaded 
for i in link:
    yt=YouTube(i)
    yt.streams.first().download()
