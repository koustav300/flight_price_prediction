from other_functions import UDF_func as udf



class vedio:
    def __init__(self, vdo_link, vdo_len_in_sec):
        self.vdo_link = vdo_link
        self.vdo_id   = vdo_link.replace('https://www.youtube.com/watch?v=', "")

        # fetching the vdo information from youtube api with udf
        vdo_allInfo = udf.video_info_basic(self.vdo_id)

        self.title  = vdo_allInfo['title']
        self.channel_id = vdo_allInfo['channel_id']
        self.channel_name = vdo_allInfo['channel_title']
        self.views  = int(vdo_allInfo['view_count'])
        self.likes  = int(vdo_allInfo['like_count'])
        self.comment_count = int(vdo_allInfo['comment_count'])
        self.length = vdo_allInfo['duration']
        self.length_sec = vdo_len_in_sec
        self.thumbnail_url = vdo_allInfo['thumbnail_url']

    def create_sqlLoad_dict(self):
        output_dict = {
            'vdo_id' : self.vdo_id,
            'link' : self.vdo_link,
            'channel_name' : self.channel_name,
            'view_count' : self.views,
            'like_count' : self.likes,
            'comment_count' : self.comment_count,
            'length_in_sec' : self.length_sec,
            'length' : self.length,
            'thumbnail': self.thumbnail_url
        }
        return output_dict


    def create_comment_info_dict(self):
        try:
            thumbnail_img   = udf.download_thubnail_img(self.thumbnail_url, self.vdo_id)
            comment_details = udf.video_info_comments(self.vdo_id)
            comment_details = {self.vdo_id : {"comments": comment_details,
                                              'thumbnail': thumbnail_img } }
        except Exception as e:
            print('ERROR', e)

        return comment_details


    def download_thumbnail_image(self):
        pass


#df_comment = pd.DataFrame(mongo_upload_list['comment'])


