CONTENT
        1. INTRODUCTION
        2. FILE MANIFEST
        3. INSTALLATION
        4. USAGE

1. INTRODUCTION
Obtain specific tables from bookingsync APIs and fill into DB then aplying some filtering upload the data from db to Bitrix CRM


2.FILE MANIFEST
configs.ini - configuration file
src - python source codes
token.json - authorization rules from bookingsync  (http://developers.bookingsync.com/reference/testing_authorization/)

3. INSTALLATION
No need for any specific installation

4. USAGE
The appliation is run based on configuration files, which have special rules to change the application behaviour.

configs/bitrix.ini

    probability_win_interval - this sesction describes win probability of specific day intervals (for the usage instructions see bitrix.ini )
    update_fields - column names which are exceptions and should be updated each time they are being changed in DB.
    other_fields - secondary configuration flags
        payed_status_interval - is being used for the stage_id and if start_at - current_date <= payed_status_interval then stage_id=Payed
                                possible values are any integer
        remove_old_rows - if set "Yes", the program removes the row in case it exists in Bitrix but not in DB
        product_section_id - Bitrix product section ID
        clean_before_insert - does nothing, reserved for further implementation
    
    bitrix24_auth - this section is related to authentication and is not user related
    
    
    
configs/bookinsync.ini
    
    bookingync - this section is not interesting for user, the only user related fields is described below 
            clean_before_insert - if set yes(true or 1) all DB data should be removed and uploaded again
                                  Note: use this if you have an issue and need to reload all table datas
    fee_mapping - english to any language mapping for the feelds related to fee names
    price_splitting - specifies whether a specific payement should be splitted in bookings_split table or not
    
    
        

IN CASE OF QUESTIONS PLEASE CONTACT: ikhachatryan93@gmail.com
