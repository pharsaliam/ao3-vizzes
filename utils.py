import logging

import numpy as np


CSV_WITH_TAG_DICT_NAME = 'works_df_with_parsed_tags_test.csv'
LOGGING_LEVEL = logging.INFO
FANDOM = 'Marvel Cinematic Universe'
WORKS_CSV = 'ao3_official_dump_210321/works-20210226.csv'
TAGS_CSV = 'ao3_official_dump_210321/tags-20210226.csv'

logger = logging.getLogger('LOG')
logger.setLevel(LOGGING_LEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'
)
ch.setFormatter(formatter)
logger.addHandler(ch)


def create_tag_dict(tag_list, tag_lookup_table):
    """
    Creates a list of dictionaries denoting the ID and type of tag for all tags that can be found in a tag lookup
    :param tag_list: A list of tag IDs
    :param tag_lookup_table: A DataFrame containing information for tags
    :return: A list of dictionaries where the key, value pairs notes the tag ID and type
    """
    tag_dict_list = []
    # Check to make sure that the tag list isn't empty
    if not isinstance(tag_list, float):
        for tag_id in tag_list:
            tag_dict = None
            tag_id = int(tag_id)
            if tag_id in tag_lookup_table.index:
                tag_dict = {'id': tag_id, 'type': tag_lookup_table.loc[tag_id, 'type']}
                tag_dict_list.append(tag_dict)

    return tag_dict_list


def parse_tags_by_type(tag_dict_list, tag_type, tag_lookup_table):
    """
    Parses tag IDs by type and retrieves a list of tag names
    :param tag_dict_list: A list of dictionaries where the key, value pairs notes the tag ID and type
    :param tag_type: The type of tag
    :param tag_lookup_table: A DataFrame containing information for tags
    :return: A list of tag names
    """
    tag_name_list = []
    for td in tag_dict_list:
        try:
            tag_name = ''
            if td['type'] == tag_type:
                canon_tag_id = tag_lookup_table.loc[td['id'], 'merger_id']
                # if there is a canonical version of the tag, retrieve that instead
                if not np.isnan(canon_tag_id):
                    tag_name = tag_lookup_table.loc[canon_tag_id, 'name']
                else:
                    tag_name = tag_lookup_table.loc[td['id'], 'name']
            if tag_name:
                tag_name_list.append(tag_name)
        except KeyError:
            # Not sure how, but a few stories have tags that aren't included in the tag_df
            print(td)
            pass
    return tag_name_list


