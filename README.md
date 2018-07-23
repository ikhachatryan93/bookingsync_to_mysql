CONTENT
        1. INTRODUCTION
        2. FILE MANIFEST
        3. INSTALLATION
        4. USAGE
        5. PREREQUISITES

1. INTRODUCTION
This tool does two data transmission:
  . Bookingsync to MySQL synchronization - obtains specific tables from bookingsync account and fills into DB.
  . MySQL to Bitrix synchronization      - processes the data from db and uploads to Bitrix CRM account


2.FILE MANIFEST
configs/       : configuration files
src/           : python source codes
tokens/        : authorization rules for bookingsync and bitrix api services 


3. INSTALLATION
. place the files in any direcotry
. generate bookingsync access token and put in bitrix (http://developers.bookingsync.com/reference/testing_authorization/)
. generate bitrix24 access token (https://training.bitrix24.com/rest_help/oauth/app_authentication.php)


4. USAGE
The appliation is being run based on configuration rules.

configs/bitrix.ini

    probability_win_interval - this section describes win probability of specific day intervals
    update_fields - describes the exception columns which should be updated each time they are being changed in DB.
    other_fields - secondary configuration flags
        payed_status_interval - used for the stage_id and if start_at - now <= payed_status_interval then stage_id=Payed
        remove_old_rows - if set "Yes", tool removes old rows that are existing in bitrix but removed in db 
        product_section_id - product section ID
        clean_before_insert - does nothing, reserved for further implementation
    
    bitrix24_auth - this section is related to authentication and is not user related
    
    
    
configs/bookinsync.ini
    
    mysql      - not user related
    bookingync - not user related
    clean_before_insert - if set yes(true or 1) all DB data should be removed and uploaded again
                          Note: use this if you have an issue and need to reload all table datas
    fee_mapping - english to any language mapping for the fee names
    price_splitting - specifies whether a specific payment should be splitted in bookings_split table or not
    
    
5. PREREQUISITES

. PYTHON 3.5

Note: Some python modules may be missed in standard library, so you need manually install them.

IN CASE OF QUESTIONS PLEASE CONTACT: ikhachatryan93@gmail.com
