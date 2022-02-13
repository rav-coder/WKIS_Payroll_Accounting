/*
This anonymouse block is for sorting out double accounting transactions. It follows all the
accounting guide lines to make sure the account balance is updated correctly, A = L + (OE + RE -EX).
It will also catch all the follow errors:
a. Missing transaction number (NULL transaction number)
b. Debits and credits are not equal
c. Invalid account number
d. Negative value given for a transaction amount
e. Invalid transaction type

Authors: Yun Ze (David) Wei, Xiaomeng Li, Joshua Naymie, Saurav Adhikari
Date: Nov. 4 / 2021

*/

DECLARE
--transaction level CURSOR
CURSOR c_newtransaction IS	
	SELECT UNIQUE transaction_no, transaction_date, description
	FROM new_transactions
	ORDER BY transaction_no;

--variable to hold transaction number
v_transactionno 		new_transactions.transaction_no%TYPE;

--row per transaction cursor
CURSOR c_transactionrows IS
	SELECT account_no, transaction_type, transaction_amount
	FROM new_transactions
	WHERE transaction_no = v_transactionno;

--variables for error checking
v_defaulttransactiontype 	account_type.default_trans_type%TYPE;
v_accbalancechange 			account.account_balance%TYPE;
v_err_msg VARCHAR2(60);
v_numberofaccounts NUMBER;
v_debitequalscreditck NUMBER;

--Exception variable
ex_missing_transaction_number 	EXCEPTION;
ex_negative_transaction_amount EXCEPTION;

--constants
k_transaction_type_debit CONSTANT CHAR(1)  := 'D';
k_transaction_type_credit CONSTANT CHAR(1)  := 'C';

BEGIN
--outer loop for trans lvl CURSOR
FOR r_newtransaction IN c_newtransaction LOOP
	--embedded blck for exception handling
	BEGIN
	--assign transaction number variable
	v_transactionno := r_newtransaction.transaction_no;
	
	--used for debit = credit check, should stay 0 the whole time if debit = credit.
	v_debitequalscreditck := 0;
	
	--checking for null transaction number 
	IF (v_transactionno IS NULL) THEN
		RAISE ex_missing_transaction_number;
	END IF;
	
	--inserting into transaction_history
	INSERT INTO transaction_history (transaction_no, transaction_date, description)
		VALUES (r_newtransaction.transaction_no, r_newtransaction.transaction_date,
				r_newtransaction.description);
		
	--DBMS_OUTPUT.PUT_LINE(r_newtransaction.transaction_no);
	
	-- inner loop for row searching the transaction 
	FOR r_transactionrows IN c_transactionrows LOOP
		--checking for a negative amount inside transaction
		IF(r_transactionrows.transaction_amount < 0) THEN
			RAISE ex_negative_transaction_amount;
		END IF;
		
	    --Checks to see that account number is valid
		SELECT COUNT(*)
			INTO v_numberofaccounts
			FROM ACCOUNT
		WHERE ACCOUNT_NO = r_transactionrows.account_no;
		
		IF(v_numberofaccounts = 0) THEN
			RAISE_APPLICATION_ERROR(-20010, 'Invalid Account - #' || r_transactionrows.account_no);
		END IF;
		
		-- check transaction type
		IF (r_transactionrows.transaction_type != k_transaction_type_debit AND r_transactionrows.transaction_type != k_transaction_type_credit) THEN
                RAISE_APPLICATION_ERROR(-20005, 'Invalid transaction type');
        END IF;
		
		--check if debit equals debit
		IF (r_transactionrows.transaction_type = k_transaction_type_debit) THEN
			v_debitequalscreditck := v_debitequalscreditck + r_transactionrows.transaction_amount;
		ELSIF (r_transactionrows.transaction_type = k_transaction_type_credit) THEN
			v_debitequalscreditck := v_debitequalscreditck - r_transactionrows.transaction_amount;
		END IF;
			
		--inserting into transaction_detail
		INSERT INTO transaction_detail (account_no, transaction_no,transaction_type,
					transaction_amount)
			VALUES (r_transactionrows.account_no, v_transactionno, r_transactionrows.transaction_type,
					r_transactionrows.transaction_amount);
					
		--assigning default transaction type for account_no the cursor is pointing to
		SELECT default_trans_type
			INTO v_defaulttransactiontype
			FROM account a, account_type at
		WHERE a.account_type_code = at.account_type_code AND account_no = r_transactionrows.account_no;
		
		--figuring out to add or subtract from account balance
		CASE
			WHEN (v_defaulttransactiontype = r_transactionrows.transaction_type) THEN
				v_accbalancechange := r_transactionrows.transaction_amount;
			ELSE
				v_accbalancechange := -1 * r_transactionrows.transaction_amount;
		END CASE;
		
		--Update account balance
		UPDATE account 
			set account_balance = account_balance + v_accbalancechange
		WHERE account_no = r_transactionrows.account_no;
				
		--DBMS_OUTPUT.PUT_LINE(r_transactionrows.account_no);
		--DBMS_OUTPUT.PUT_LINE(v_defaulttransactiontype);
		--DBMS_OUTPUT.PUT_LINE(v_accbalancechange);
	END LOOP;
	
	--throw exception if debit != credit
	IF (v_debitequalscreditck != 0 ) THEN
		RAISE_APPLICATION_ERROR (-20015, 'Debit does not equal to credit for transaction number: ' || r_newtransaction.transaction_no);
	END IF;
	
	--Delete the transaction as a whole unit
	DELETE FROM new_transactions
		WHERE transaction_no = r_newtransaction.transaction_no;
		
	COMMIT;
	
	--Main exception block 
	EXCEPTION
		--Error thrown for NULL transaction numbers
		WHEN ex_missing_transaction_number THEN
			DBMS_OUTPUT.PUT_LINE('Missing transaction number for: ' || r_newtransaction.description || ' , done on: ' || r_newtransaction.transaction_date);
			ROLLBACK;
			INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
				VALUES (r_newtransaction.transaction_no, r_newtransaction.transaction_date,
				r_newtransaction.description, 'Missing transaction number for: ' || r_newtransaction.description || ' , done on: ' ||
				r_newtransaction.transaction_date);
			COMMIT;
		--Error thrown for negative transaction amount
		WHEN ex_negative_transaction_amount THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('Negative value for a transaction amount. Transaction not added.');
            INSERT INTO WKIS_ERROR_LOG (TRANSACTION_NO, TRANSACTION_DATE, DESCRIPTION, ERROR_MSG)
            VALUES (r_newtransaction.transaction_no, r_newtransaction.transaction_date, r_newtransaction.description, 'Negative value for a transaction amount. Transaction not added.');   
            COMMIT;
		--Error thrown for debit != credit, Invalid account number, Invalid transaction type, all other unanticipated errors
		WHEN OTHERS THEN
			ROLLBACK;
            v_err_msg := SUBSTR(SQLERRM, 12, 60);
			INSERT INTO wkis_error_log
				VALUES(r_newtransaction.transaction_no, r_newtransaction.transaction_date, r_newtransaction.description, v_err_msg);
			DBMS_OUTPUT.PUT_LINE(SQLERRM);				
			COMMIT;			
	--end of embedded block
	END;
	
END LOOP;

END;
/