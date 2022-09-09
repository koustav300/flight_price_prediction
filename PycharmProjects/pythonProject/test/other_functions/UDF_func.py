import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd


# AIzaSyC5vrAqcbk1QcdXJBx1zXsChV4EgPnDFhQ


def extract_vdo_comment(ui_link, ui_thumbnail_url):
    options = Options()
    options.add_argument("start-maximized")

    # path to chrome driver
    driver = webdriver.Chrome(options=options, executable_path='../chromedriver.exe')
    driver.get(ui_link)

    wait = WebDriverWait(driver, 15)

    # going to end of page to ensure all comments are loaded
    for item in range(25):
        wait.until(EC.visibility_of_element_located((By.TAG_NAME, "body"))).send_keys(Keys.END)
        # for comment in wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#content"))):

    # Getting the source formatted html
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # extracting no of likes
    total_likes = soup.find_all("div", id="top-level-buttons-computed")[0].find_all("yt-formatted-string")[0].text
    print('The total likes for this vdo is - ', total_likes)

    # Scrapping the comment info
    no_of_comments = 0
    comments_list = []
    commenter_name_list = []

    comment_mainContainer = soup.find_all("ytd-comment-thread-renderer")
    for comment_div in comment_mainContainer:
        comment_text = comment_div.find_all(id="content-text")[0].text
        commenter_name = comment_div.find_all(id='author-text')[0].find_all('span')[0].text
        print('----------------------------- ', commenter_name)
        print(comment_text)
        comments_list.append(comment_text)
        commenter_name_list.append(commenter_name.strip())
        no_of_comments = no_of_comments + 1

    # downloading thubnail img
    img_content = requests.get(ui_thumbnail_url).content

    target_path = '../images'
    target_folder = os.path.join(target_path, '_'.join('test'))
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    counter = 1
    f = open(os.path.join(target_folder, 'jpg' + "_" + str(counter) + ".jpg"), 'wb')
    f.write(img_content)
    f.close()

    return [int(total_likes), int(no_of_comments), comments_list, commenter_name_list]


# -----------------------------------------------------------------------------------------------

from googleapiclient.discovery import build

api_key = 'AIzaSyC5vrAqcbk1QcdXJBx1zXsChV4EgPnDFhQ'


def video_info_comments(video_id):
    # empty list for storing reply
    comment_list = {}

    # creating youtube resource object
    youtube = build('youtube', 'v3',
                    developerKey=api_key)

    # retrieve youtube video results
    video_response = youtube.commentThreads().list(
        part='snippet,replies',
        videoId=video_id
    ).execute()

    # np =video_response['nextPageToken']
    # print(np)
    # return 'sdf'

    # iterate video response
    comment_counter = 0
    while video_response:

        # extracting required info
        # from each result object

        for item in video_response['items']:

            # Extracting comments
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            commeter = item['snippet']['topLevelComment']['snippet']['authorDisplayName']

            # append to output dict
            # adding the counter as one per may comment multiple times
            comment_list[str(comment_counter)] = {
                'comment': {commeter: comment},
                'reply': {}
            }

            # counting number of reply of comment
            replycount = item['snippet']['totalReplyCount']

            # if reply is there
            reply_list = []
            if replycount > 0:

                # iterate through all reply
                for reply in item['replies']['comments']:
                    # Extract reply
                    reply_text = reply['snippet']['textDisplay']
                    replyer = reply['snippet']['authorDisplayName']

                    # Adding replies to the output comment
                    comment_list[str(comment_counter)]['reply'][str(replycount)] = {replyer: reply_text}

            # increasing the counter
            comment_counter = comment_counter + 1

        # Again repeat
        if 'nextPageToken' in video_response:
            video_response = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id,
                pageToken=video_response["nextPageToken"],
            ).execute()
        else:
            break
    print('comment_list')
    return comment_list


def video_info_basic(video_id):
    # creating youtube resource object
    youtube = build('youtube', 'v3',
                    developerKey=api_key)

    # retrieve youtube video results
    video_response = youtube.videos().list(
        part='snippet, statistics, contentDetails',
        id=video_id
    ).execute()

    title = video_response['items'][0]['snippet']['title']
    channel_id = video_response['items'][0]['snippet']['channelId']
    thumbnail_url = video_response['items'][0]['snippet']['thumbnails']['high']['url']  # default
    channel_title = video_response['items'][0]['snippet']['channelTitle']
    like_count = video_response['items'][0]['statistics']['likeCount']
    view_count = video_response['items'][0]['statistics']['viewCount']
    comment_count = video_response['items'][0]['statistics']['commentCount']
    duration = video_response['items'][0]['contentDetails']['duration']

    output_dict = {
        'title': title,
        'channel_id': channel_id,
        'thumbnail_url': thumbnail_url,
        'channel_title': channel_title,
        'like_count': like_count,
        'view_count': view_count,
        'comment_count': comment_count,
        'duration': duration
    }
    return output_dict


def download_thubnail_img(ui_thumbnail_url, img_name):
    # downloading thubnail img
    img_content = requests.get(ui_thumbnail_url).content

    # os.chdir('..')
    # target_path = os.getcwd()
    # target_folder = os.path.join(target_path, 'test','static', 'images')
    target_folder = os.path.join('../test', 'static', 'images')
    f = open(os.path.join(target_folder, img_name + ".jpg"), 'wb')
    f.write(img_content)
    f.close()
    return img_content


def download_imgFrom_mongodb(UI_img_content):
    # downloading thubnail img
    img_content = UI_img_content

    os.chdir('..')
    target_path = os.getcwd()
    target_folder = os.path.join(target_path, 'test', 'static', 'images')
    target_folder = os.path.join('../test', 'static', 'images')

    counter = 2
    f = open(os.path.join(target_folder, 'jpg' + "_" + str(counter) + ".jpg"), 'wb')
    f.write(img_content)
    f.close()


def yt_video_len_in_sec(input_api_len):
    L = list(input_api_len)
    for i in range(len(L)):
        if L[i].isdigit():
            pass
        else:
            L[i] = "-"
    modified_string = ''.join(L).rstrip('-').lstrip('-')
    new_l = modified_string.split('-')

    # converting duration to Sec
    len_in_sec = 0
    if len(new_l) == 3:  # H+M+S
        len_in_sec = ((int(new_l[0]) * 60) * 60) + (int(new_l[1]) * 60) + int(new_l[2])
    elif len(new_l) == 2:  # M+S
        len_in_sec = (int(new_l[1]) * 60) + int(new_l[2])
    else:  # S
        len_in_sec = int(new_l[2])

    return len_in_sec


def fetch_scrapped_info_frmMysql(input_channel_name):
    from other_functions import UDF_connections as udf_con

    # creating pySQL conncetion
    conn = udf_con.create_pysql_connction()

    # creating query string
    sql_sqtring = f"Select * from basic_scrap_info where channel_name = '{input_channel_name}'"
    print(sql_sqtring)

    # create cursor  & Execute query
    cursor = conn.cursor()
    cursor.execute(sql_sqtring)

    # Fetch all the records
    result = cursor.fetchall()
    html_text = ""
    for row in result:
        html_text += "<tr class='tr_type_1'>"  # opening the row

        for row_value in range(0, len(row)):
            if row_value == 8:
                html_text += f"<td class='td_thumbnail'> <img src='{row[row_value]}' alt='thumbnail' srcset=''> </td>"
            else:
                html_text += f"<td>{row[row_value]}</td>"  # creating tds

        html_text += "</tr>"  # closing the row

    conn.close()

    return html_text


def fetch_scrapped_info_frmMongoDb(input_channel_name):
    import time
    from other_functions import UDF_connections as new_conn
    # connecting to mongoDb
    try:
        client = new_conn.create_mongodb_conn()
    except Exception as e:
        print("ERROR: Fail to connect to mongoDb")
        print(e)

    db = client['mongotest']
    collection = db['testLoadtest5']
    cursor = collection.find({'channel_name': input_channel_name})
    print('S3.1')

    html_text = ''
    for chnl in cursor:
        print(print('S3.2'))
        # getting the record of this channel
        all_vdo_info = chnl['list_of_vdos']  # return arry with object-per-vdo
        vdo_ids = list(all_vdo_info.keys())

        # for each vdo-id in the channel
        for vdo_id in vdo_ids:
            comments = all_vdo_info[vdo_id]['comments']

            # for each comment in among the comments
            for key in comments:
                comment = comments[key]['comment']
                reply = comments[key]['reply']

                commenter = list(comment.items())[0][0]
                commentText = list(comment.items())[0][1]
                # print(commenter, ' - ', commentText)
                html_text += f"<tr> <td>{vdo_id}</td> <td>{commenter}</td> <td>{commentText}</td> <td><button class='btn_showtext'>Show details</button></td> </tr>"

    print(html_text)
    return html_text


def getting_aws_credentials():
    import boto3
    # import base64
    from botocore.exceptions import NoCredentialsError
    from botocore.exceptions import ClientError

    secret_name = "new_user_sm"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    print(client)
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.

            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            # print(secret)
        else:
            # decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            pass

    return secret
