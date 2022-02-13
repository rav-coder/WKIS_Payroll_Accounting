DECLARE
-- Retrieves data that requires processing
  CURSOR c_gggs IS
    SELECT *
      FROM gggs_data_upload
      FOR UPDATE;
      
-- Constants (different data types to process)
  k_customer        CONSTANT    gggs_data_upload.data_type%TYPE := 'CU';
  k_vendor          CONSTANT    gggs_data_upload.data_type%TYPE := 'VE';
  k_category        CONSTANT    gggs_data_upload.data_type%TYPE := 'CA';
  k_stock           CONSTANT    gggs_data_upload.data_type%TYPE := 'ST';  
  
-- Constants (different actions)
  k_new             CONSTANT    gggs_data_upload.process_type%TYPE := 'N';
  k_status          CONSTANT    gggs_data_upload.process_type%TYPE := 'S';
  k_change          CONSTANT    gggs_data_upload.process_type%TYPE := 'C'; 

-- Constants (other)
  k_active_status   CONSTANT    gggs_customer.status%TYPE := 'A';
  k_no_change_char  CONSTANT    CHAR(2) := 'NC';
  k_no_change_numb  CONSTANT    NUMBER := -1;  
  
-- Working variables
  v_name1                       gggs_stock.name%TYPE;
  v_name2                       gggs_stock.name%TYPE;  
  v_rowsfound                   NUMBER;
  v_category_rowsfound          NUMBER;
  v_vendor_rowsfound            NUMBER;
BEGIN

  FOR r_gggs IN c_gggs LOOP
	  SELECT COUNT(*) 
      INTO v_rowsfound 
      FROM gggs_customer 
      WHERE NAME = r_gggs.column1; 
	  
	  SELECT COUNT(*) 
      INTO v_category_rowsfound 
      FROM gggs_category 
      WHERE NAME = r_gggs.column1;
	  
	  SELECT COUNT(*) 
      INTO v_vendor_rowsfound 
      FROM gggs_vendor 
      WHERE NAME = r_gggs.column1;

	  
-- customer
    IF (r_gggs.data_type = k_customer) THEN
    -- new customer
      IF (r_gggs.process_type = k_new AND v_rowsfound = 0) THEN
        INSERT INTO gggs_customer
        VALUES (gggs_customer_seq.NEXTVAL, r_gggs.column1, r_gggs.column2, r_gggs.column3,
                r_gggs.column4, r_gggs.column5, r_gggs.column6, k_active_status);
      
    -- customer status change  
      ELSIF (r_gggs.process_type = k_status) THEN
        UPDATE gggs_customer
           SET status = r_gggs.column2
         WHERE name = r_gggs.column1;
            
    -- customer information changes  
      ELSIF (r_gggs.process_type = k_change) THEN
        UPDATE gggs_customer
           SET province = DECODE(r_gggs.column2, k_no_change_char, province, r_gggs.column2),
               first_name = DECODE(r_gggs.column3, k_no_change_char, first_name, r_gggs.column3),
               last_name = DECODE(r_gggs.column4, k_no_change_char, last_name, r_gggs.column4),
               city = DECODE(r_gggs.column5, k_no_change_char, city, r_gggs.column5),
               phone_number = NVL2(r_gggs.column6, r_gggs.column6, phone_number)
         WHERE name = r_gggs.column1;  
      
      END IF;
  
  
-- vendor
    ELSIF (r_gggs.data_type = k_vendor) THEN
    -- new vendor
      IF (r_gggs.process_type = k_new AND v_vendor_rowsfound = 0) THEN
        INSERT INTO gggs_vendor
        VALUES (gggs_vendor_seq.NEXTVAL, r_gggs.column1, r_gggs.column2, r_gggs.column3,
                r_gggs.column4, r_gggs.column6, k_active_status);      
                
    -- vendor status change  
      ELSIF (r_gggs.process_type = k_status) THEN
        UPDATE gggs_vendor
           SET status = r_gggs.column2
         WHERE name = r_gggs.column1;    
      
    -- vendor information changes  
      ELSIF (r_gggs.process_type = k_change) THEN
        UPDATE gggs_vendor
           SET description = DECODE(r_gggs.column2, k_no_change_char, description, r_gggs.column2),
               contact_first_name = DECODE(r_gggs.column3, k_no_change_char, contact_first_name, r_gggs.column3),
               contact_last_name = DECODE(r_gggs.column4, k_no_change_char, contact_last_name, r_gggs.column4),
               contact_phone_number = NVL2(r_gggs.column6, contact_phone_number, r_gggs.column6)
         WHERE name = r_gggs.column1;  
      
      END IF;


-- category
    ELSIF (r_gggs.data_type = k_category) THEN
    -- new category
      IF (r_gggs.process_type = k_new AND v_category_rowsfound = 0) THEN
        INSERT INTO gggs_category
        VALUES (gggs_category_seq.NEXTVAL, r_gggs.column1, r_gggs.column2, k_active_status);
                
    -- category status change  
      ELSIF (r_gggs.process_type = k_status) THEN
        UPDATE gggs_category
           SET status = r_gggs.column2
         WHERE name = r_gggs.column1;
      
      END IF;


-- stock
    ELSIF (r_gggs.data_type = k_stock) THEN
    -- new stock information
      IF (r_gggs.process_type = k_new) THEN
        SELECT categoryID
          INTO v_name1
          FROM gggs_category
         WHERE name = r_gggs.column1;
         
        SELECT vendorID
          INTO v_name2
          FROM gggs_vendor
         WHERE name = r_gggs.column2;     
      
        INSERT INTO gggs_stock
        VALUES (gggs_stock_seq.NEXTVAL, v_name1, v_name2, r_gggs.column3,
                r_gggs.column4, r_gggs.column7, r_gggs.column8, k_active_status);
                
    -- stock status change  
      ELSIF (r_gggs.process_type = k_status) THEN
        UPDATE gggs_stock
           SET status = r_gggs.column2
         WHERE name = r_gggs.column1;
      
      
    -- stock information changes  
      ELSIF (r_gggs.process_type = k_change) THEN
        UPDATE gggs_stock
           SET description = DECODE(r_gggs.column4, k_no_change_char, description, r_gggs.column4),
               price = NVL2(r_gggs.column7, r_gggs.column7, price),
               no_in_stock = NVL2(r_gggs.column8, (no_in_stock + r_gggs.column8), no_in_stock)
         WHERE name = r_gggs.column1;  
      
      END IF;

    END IF;
    
    DELETE gggs_data_upload
     WHERE CURRENT OF c_gggs;
    
  END LOOP;  

-- Save data changes    
  COMMIT;  
  
END;
/
