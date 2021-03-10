import csv
import logging
from datetime import datetime
from xml.etree import ElementTree
from xml.etree.ElementTree import iselement, Element
from zipfile import ZipFile

import boto3
import requests
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import TransferConfig

from botocore.exceptions import BotoCoreError

# define your s3 bucket name
BUCKET_NAME = 'BUCKET_NAME'

logger = logging.getLogger(__name__)


def upload_to_s3(file_name: str) -> bool:
    """
    This function is use for uploading a file to s3 bucket.
    # TODO: please add bucket name and aws credentials to work properly
    :param file_name: file path which you want to upload on s3
    :return: bool , For success: True and for fail: return False
    """
    try:
        s3_client = boto3.client('s3')
        config = TransferConfig(multipart_threshold=1024 * 25,
                                max_concurrency=10,
                                multipart_chunksize=1024 * 25,
                                use_threads=True)
        obj_name = f"{int(datetime.now().timestamp())}.csv"
    except BotoCoreError:
        logger.error("S3 client initial failed", exc_info=True)
        return False
    try:
        logger.info(f"Trying to upload csv file to s3 bucket {file_name}")
        s3_client.upload_file(file_name, BUCKET_NAME, obj_name, Config=config)
        logger.info(f"Successfully Uploaded file {file_name} to s3 bucket")
        return True
    except S3UploadFailedError:
        logger.error(f"S3 file uploading failed {file_name}", exc_info=True)
        return False


def remove_namespace(xml_iterator):
    """
    This function will remove all namespace from your xml tree
    :param xml_iterator: an iterator obj
    :return: cleaned up iterator obj
    """
    for _, el in xml_iterator:
        prefix, has_namespace, postfix = el.tag.rpartition('}')
        if has_namespace:
            el.tag = postfix  # strip all namespaces
    root = xml_iterator
    return root


def xml_to_csv(xml_tree) -> str:
    """
    From given xml tree, create new csv file
    :param xml_tree: XML tree
    :return: csv file name
    """
    csv_header = ['FinInstrmGnlAttrbts.Id',
                  'FinInstrmGnlAttrbts.FullNm',
                  'FinInstrmGnlAttrbts.ClssfctnTp',
                  'FinInstrmGnlAttrbts.CmmdtyDerivInd',
                  'FinInstrmGnlAttrbts.NtnlCcy',
                  'Issr']
    try:
        with open('file.csv', 'w') as csv_dict_writer:
            writer = csv.DictWriter(csv_dict_writer, fieldnames=csv_header)
            writer.writeheader()

            for record in xml_tree.iter('TermntdRcrd'):
                main_block = record.find('FinInstrmGnlAttrbts')
                data_dict = dict()
                if iselement(main_block):
                    data_dict['FinInstrmGnlAttrbts.Id'] = main_block.find("Id").text \
                        if iselement(main_block.find("Id")) else None
                    data_dict['FinInstrmGnlAttrbts.FullNm'] = main_block.find("FullNm").text \
                        if iselement(main_block.find("FullNm")) else None
                    data_dict['FinInstrmGnlAttrbts.ClssfctnTp'] = main_block.find("ClssfctnTp").text \
                        if iselement(main_block.find("ClssfctnTp")) else None
                    data_dict['FinInstrmGnlAttrbts.CmmdtyDerivInd'] = main_block.find("CmmdtyDerivInd").text \
                        if iselement(main_block.find("CmmdtyDerivInd")) else None
                    data_dict['FinInstrmGnlAttrbts.NtnlCcy'] = main_block.find("NtnlCcy").text \
                        if iselement(main_block.find("NtnlCcy")) else None
                data_dict['Issr'] = record.find('Issr').text if iselement(record.find('Issr')) else None
                writer.writerow(data_dict)
        logger.info('Successfully csv fle is created {file.csv}')
        return 'file.csv'
    except IOError:
        logger.error("Not able to create a csv file or IO bound error", exc_info=True)
        raise IOError


def get_xml_tree_from_zip(url: str) -> Element or None:
    """
    This function download the zip and extract zip and return xml tree
    :param url: url which to download
    :return: xml tree
    """
    try:
        response = requests.get(url)
    except ConnectionError:
        logger.error(f"network problems for url {url}", exc_info=True)
        raise None
    if response.status_code == 200:
        try:
            with open('file.zip', 'wb') as f:
                f.write(response.content)
        except IOError:
            logger.error(f"Not able to write zip file to local for url {url}", exc_info=True)
            return None
        try:
            zip_file = ZipFile('file.zip')
            zip_file.extractall()
            extract_files = zip_file.namelist()
            logger.info(f"Zip is extracted proper for {url}")
            for file in extract_files:
                xml_iterator = ElementTree.iterparse(file)
                xml_cleaned_iterator = remove_namespace(xml_iterator)
                root = xml_cleaned_iterator.root
                return root
        except Exception:
            logger.error(f"Not able to extract zip file or convert to xml tree for {url}", exc_info=True)
    else:
        logger.error(f"Request is not proper. got status code ={response.status_code}, {response.content}",
                     exc_info=True)
        return None


def download_main_file():
    """
    The main function to handle all
    :return:
    """
    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
    params = {
        "q": "*",
        "fq": "publication_date:[2021-01-17T00:00:00Z TO 2021-01-19T23:59:59Z]",
        "wt": "xml",
        "indent": "true",
        "start": "0",
        "rows": "100"
    }
    try:
        response = requests.get(url=url, params=params)
        logger.info(f"Successfully request is connected {url}")
    except ConnectionError:
        logger.error("Not able to connect to internet", exc_info=True)
        return False
    if response.status_code == 200:
        tree = ElementTree.fromstring(response.content.decode())
        all_docs = tree.find('result').findall("doc")
        result_doc = None
        for doc in all_docs:
            if doc.find("str[@name='file_type']").text == 'DLTINS':
                result_doc = doc
                break
        if not iselement(result_doc):
            logger.info("Did not get any file type as `DLTINS`")
            return False
        download_doc = result_doc.find("str[@name='download_link']")
        if iselement(download_doc):
            download_link = download_doc.text
            xml_tree_root = get_xml_tree_from_zip(download_link)
            if not iselement(xml_tree_root):
                logger.info("Zip is not converted to xml tree")
                return False
            csv_file = xml_to_csv(xml_tree_root)
            if not csv_file:
                return False
            # TODO: uncomment below line to upload s3 bucket
            # status = upload_to_s3(csv_file)
            # logger.info("All task is done")
            # return status
        else:
            logger.warning("Did not found any download link to download zip")
            return False
    else:
        logger.error(f"Request is not proper. got status code ={response.status_code}, {response.content}",
                     exc_info=True)
        return False


if __name__ == '__main__':
    logger.info(f" all over output: {download_main_file()}")
