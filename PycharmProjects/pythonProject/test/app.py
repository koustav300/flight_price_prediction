from flask import Flask, request, render_template, jsonify, send_file, send_from_directory
from pytube import YouTube
from pytube import Channel
from other_functions import UDF_func as udf, UDF_connections as con, oops_file as oops
import boto3
from botocore.exceptions import NoCredentialsError
import os
import urllib.request
from werkzeug.utils import secure_filename
import pandas as pd
import zipfile

app = Flask(__name__)


@app.route('/', methods=['GET'])
def homepage():
    return render_template("index.html")


@app.route('/scrap_new_request', methods=['GET'])
def new_scrap_request():
    channel_url = request.args.get('channel_name')

    try:  # trying to fetch vdo urls of the channel
        channel_url = Channel(channel_url)  # pytube processing
    except:
        return "ERROR - Could not collect the channel info. Make sure the channel url is valid "

    # no of videos  user wants fetch the data of
    vdo_limit = int(request.args.get('target_nunOf_vdos'))
    target_vdo_len = int(request.args.get('target_length'))
    print(type(vdo_limit), type(target_vdo_len))

    # list for hold data for all the videos
    sql_upload_list = []
    mongo_upload_list = []
    mongo_upload_dict = {'channel_name': channel_url.channel_name,
                         'list_of_vdos': {}}

    counter = 0
    for vdo_url in channel_url.video_urls:
        yt = YouTube(vdo_url)
        print(vdo_url)

        # checking the length of the vdo from pytube api -- return in sec
        vdo_len = yt.length / 60

        # considering videos with user given target length
        if vdo_len < target_vdo_len:
            # creating new Object
            new_vdo = oops.vedio(vdo_url, vdo_len)

            # calling object function -- create a dict with all info that will be loaded in mysql
            sql_upload_list.append(new_vdo.create_sqlLoad_dict())
            print('s2----', sql_upload_list)

            # calling object function -- create a dict with all info that will be loaded in mongodb
            mongo_upload_list.append(new_vdo.create_comment_info_dict())
            x = mongo_upload_dict['list_of_vdos']
            x.update(new_vdo.create_comment_info_dict())
            print('s3----')

            counter = counter + 1

        if counter > vdo_limit - 1:
            break  # if the limit is reached

    # creating df from all the vdo info
    df = pd.DataFrame(sql_upload_list)
    print('s4')

    # Uploading info to mysql
    try:
        engine = con.create_sql_engine()
        df.to_sql('basic_scrap_info', engine, if_exists='append', index=False)
        # engine.dispose()
        print('s5')
    except:
        return 'ERROR- mysql connection error, fail to insert video information to mysql'

    # connecting to mongoDb
    try:
        client = con.create_mongodb_conn()
        print('s6')
    except:
        return "ERROR: Fail to connect to mongoDb"

    # uploading to mongoDb
    try:
        db = client['mongotest']
        collection = db['testLoadtest5']
        # collection.insert_one({channel_url.channel_name: mongo_upload_list})
        collection.insert_one(mongo_upload_dict)
        print('s7')
    except:
        return "ERROR: Fail to insert data to mongoDb"

    return 'Data is loaded successfully'


@app.route('/fetch_dataFromDb', methods=['GET'])
def fetchDataFromDb():
    channel_url = request.args.get('channel_name')
    print('S1')
    try:  # trying to fetch vdo urls of the channel
        channel = Channel(channel_url)  # pytube processing
        chnnl_name = channel.channel_name
    except:
        return "ERROR - Could not collect the channel info. Make sure the channel url is valid "

    print('S2')
    basicInfo_table_text = udf.fetch_scrapped_info_frmMysql(chnnl_name)
    print(basicInfo_table_text)
    print('S3')
    comment_table_text = udf.fetch_scrapped_info_frmMongoDb(chnnl_name)
    print('S4')
    return jsonify({'basic_info': basicInfo_table_text,
                    'comment_info': comment_table_text})


@app.route('/download_videos', methods=['GET'])
def download_vdos():

    print('working')
    channel_url = request.args.get('channel_name')

    try:  # trying to fetch vdo urls of the channel
        channel_url = Channel(channel_url)  # pytube processing
    except:
        return "ERROR - Could not collect the channel info. Make sure the channel url is valid "

    # fetching all videos
    all_video = channel_url.videos
    print(all_video)
    # no of videos  user wants fetch the data of
    vdo_limit = int(request.args.get('target_nunOf_vdos'))
    target_vdo_len = int(request.args.get('target_length'))

    counter = 0
    for vdo in all_video:
        print(vdo)
        vdo.streams.first().download(output_path='/static/vdo_download')
        counter = counter + 1
        if counter > 1:
            break  # if the limit is reached


    # Zip file Initialization
    # zipfolder = zipfile.ZipFile('Audiofiles.zip', 'w', compression=zipfile.ZIP_STORED)  # Compression type
    #
    # # zip all the files which are inside in the folder
    # for root, dirs, files in os.walk('<foldername>'):
    #     for file in files:
    #         zipfolder.write('/static/vdo_download' + file)
    # zipfolder.close()
    #
    # return send_file('Audiofiles.zip',
    #                  mimetype='zip',
    #                  attachment_filename='Audiofiles.zip',
    #                  as_attachment=True)
    #
    # # Delete the zip file if not needed
    # os.remove("Audiofiles.zip")

    return 'Download successful'


@app.route('/upload_vdo_toS3', methods=['POST'])
def upload_VDO_ToS3():
    # -----------------------------------------------------

    # os.environ['AWS_PROFILE'] = "Profile1"
    # os.environ['AWS_DEFAULT_REGION'] = "us-west-2"

    # # Retrieves all regions/endpoints that work with S3
    # response = s3.list_buckets()

    # aws_cred =json.loads( udf.getting_aws_credentials())
    # ACCESS_KEY = aws_cred['ACCESS_KEY']
    # SECRET_KEY = aws_cred['SECRET_KEY']
    # Connectint to s3
    s3 = boto3.client('s3', region_name='us-west-2')
    bucket_name = 'yt-vdo-uploaded'
    # local_file = 'C:/Users/koust/Documents/vdo1.3gpp'
    s3_file_name = 'ProdLoad.3gpp'

    def upload_to_aws(local_file, bucket, s3_file):
        # s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
        #                   aws_secret_access_key=SECRET_KEY)
        # print('S3',s3)
        try:
            s3.upload_file(local_file, bucket, s3_file)
            print("Upload Successful")
            return 'uploaded to AWS Successful'
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False

    # --------------------------------------------------------------------

    # Uploading the vdos in static folder first
    # ------------------------------
    UPLOAD_FOLDER = 'static/uploads'
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
    ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', '3gpp'])

    def allowed_file(filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    # check if the post request has the file part
    if 'files[]' not in request.files:
        resp = jsonify({'message': 'No file part in the request'})
        resp.status_code = 400
        return resp

    files = request.files.getlist('files[]')

    errors = {}
    success = False

    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            savePath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(savePath)

            success = True
            print('Started2')
            uploaded = upload_to_aws(savePath, bucket_name, filename)

        else:
            errors[file.filename] = 'File type is not allowed'

    if success and errors:
        errors['message'] = 'File(s) successfully uploaded'
        resp = jsonify(errors)
        resp.status_code = 206
        return resp
    if success:
        resp = jsonify({'message': 'Files successfully uploaded'})
        resp.status_code = 201
        return resp
    else:
        resp = jsonify(errors)
        resp.status_code = 400
        return resp

    return 'uploaded to AWS Successful'


@app.route('/testing', methods=['GET'])
def vdoDownload():
    print('working')
    DOWNLOAD_FOLDER = 'static/vdo_download'
    app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER

    channel_url = request.args.get('channel_name')

    try:  # trying to fetch vdo urls of the channel
        channel_url = Channel(channel_url)  # pytube processing
    except:
        return "ERROR - Could not collect the channel info. Make sure the channel url is valid "

    # fetching all videos
    all_video = channel_url.videos
    print(all_video)
    # no of videos  user wants fetch the data of
    vdo_limit = int(request.args.get('target_nunOf_vdos'))
    target_vdo_len = int(request.args.get('target_length'))

    counter = 0
    for vdo in all_video:
        output_path = os.path.join(app.config['DOWNLOAD_FOLDER'])

        vdo.streams.first().download(output_path=output_path)
        counter = counter + 1
        if counter > 1:
            break  # if the limit is reached

    #Zip file Initialization
    zipfolder = zipfile.ZipFile('Audiofiles.zip', 'w', compression=zipfile.ZIP_STORED)  # Compression type

    # zip all the files which are inside in the folder
    for root, dirs, files in os.walk('static/vdo_download'):
        for file in files:
            zipfolder.write('/static/vdo_download/' + file)
    zipfolder.close()

    # return send_file('Audiofiles.zip',
    #                  mimetype='zip',
    #                  attachment_filename='Audiofiles.zip',
    #                  as_attachment=True)

    return 'vdo downloaded'

@app.route('/download')
def download():
    path = 'Audiofiles.zip'
    return send_file(path, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
