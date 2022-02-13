/**

Description:

Group members: Joshua Naymie, XiaoMeng Li, Saurav Adhikari, YunZe(David) Wei
Date: Dec 1 / 2021
*/

/**
Step 1, Function for checking for permission upon login
returns 'Y' if the user has permission, 'N' if the user does not
*/
CREATE OR REPLACE FUNCTION func_permissions_okay
RETURN VARCHAR2 IS
	
v_privilege VARCHAR2(20);

k_execute 	CONSTANT VARCHAR2(10) := 'EXECUTE';
k_filename 	CONSTANT VARCHAR2(10) := 'UTL_FILE';

BEGIN
	
	-- checks the user privilege
	SELECT privilege
		INTO v_privilege
	FROM user_tab_privs
	WHERE table_name = k_filename;
	
	-- if the user privilege is execute return Y other wise return N
	IF (UPPER(v_privilege) = k_execute) THEN
		RETURN 'Y';
	ELSE
		RETURN 'N';
	END IF;

END;
/

/**
Step 2, DML trigger to create a transaction for every row inserted
into PAYROLL_LOAD table
*/
CREATE OR REPLACE TRIGGER payroll_load_bir
	BEFORE
	INSERT 
	ON payroll_load
	FOR EACH ROW
	
DECLARE

k_accountspayable 	CONSTANT account.account_no%TYPE := 2050;
k_payrollexpense 	CONSTANT account.account_no%TYPE := 4045;

k_debit 			CONSTANT new_transactions.transaction_type%TYPE := 'D';
k_credit 			CONSTANT new_transactions.transaction_type%TYPE := 'C';

BEGIN

-- insert 2 transactions into new_transaction table one debit one credit
INSERT INTO new_transactions
(Transaction_no, Transaction_date, Description, Account_no, Transaction_type, Transaction_amount)
VALUES
(wkis_seq.NEXTVAL, :NEW.payroll_date, 'Payroll processed for employee ' || :NEW.employee_id, 
k_accountspayable, k_credit , :NEW.amount);

INSERT INTO new_transactions
(Transaction_no, Transaction_date, Description, Account_no, Transaction_type, Transaction_amount)
VALUES
(wkis_seq.CURRVAL, :NEW.payroll_date, 'Payroll processed for employee ' || :NEW.employee_id, 
k_payrollexpense, k_debit, :NEW.amount);

-- set status to G for succesful insertions
:NEW.status := 'G';

-- set status to B when errors occur
EXCEPTION
	WHEN OTHERS THEN
		:NEW.status := 'B';

END;
/

/**
Step 3, zeros out temporary account types RE and EX and add them to the 
owners equity account appropriately
*/
CREATE OR REPLACE PROCEDURE proc_month_end IS

k_expense 			CONSTANT CHAR(2) := 'EX';
k_revenue 			CONSTANT CHAR(2) := 'RE';
k_ownersequity 		CONSTANT NUMBER(4) := 5555;
k_debit 			CONSTANT new_transactions.transaction_type%TYPE := 'D';
k_credit 			CONSTANT new_transactions.transaction_type%TYPE := 'C';
k_description 		CONSTANT VARCHAR2(50) := 'Month end processing';

CURSOR c_expenseaccounts IS
	SELECT account_no, account_balance
	FROM account
	WHERE account_type_code = k_expense;
	
CURSOR c_revenueaccounts IS
	SELECT account_no, account_balance
	FROM account
	WHERE account_type_code = k_revenue;

BEGIN
	FOR r_expenseaccounts IN c_expenseaccounts LOOP
	
		IF (r_expenseaccounts.account_balance != 0) THEN
			INSERT INTO new_transactions
			(Transaction_no, Transaction_date, Description, Account_no, Transaction_type, Transaction_amount)
			VALUES
			(wkis_seq.NEXTVAL, SYSDATE, k_description, r_expenseaccounts.Account_no,
			k_credit, r_expenseaccounts.account_balance);
			
			INSERT INTO new_transactions
			(Transaction_no, Transaction_date, Description, Account_no, Transaction_type, Transaction_amount)
			VALUES
			(wkis_seq.CURRVAL, SYSDATE, k_description, k_ownersequity,
			k_debit, r_expenseaccounts.account_balance);
		END IF;
		
	END LOOP;
	
	FOR r_revenueaccounts IN c_revenueaccounts LOOP
	
		IF (r_revenueaccounts.account_balance != 0) THEN
			INSERT INTO new_transactions
			(Transaction_no, Transaction_date, Description, Account_no, Transaction_type, Transaction_amount)
			VALUES
			(wkis_seq.NEXTVAL, SYSDATE, k_description, r_revenueaccounts.Account_no,
			k_debit, r_revenueaccounts.account_balance);
			
			INSERT INTO new_transactions
			(Transaction_no, Transaction_date, Description, Account_no, Transaction_type, Transaction_amount)
			VALUES
			(wkis_seq.CURRVAL, SYSDATE, k_description, k_ownersequity,
			k_credit, r_revenueaccounts.account_balance);
		END IF;
		
	END LOOP;

END;
/

/**
Step 4, procedure that will populate a delimited file with data from 
new_transactions table. The delimiter is a comma.
*/
CREATE OR REPLACE PROCEDURE proc_export_csv 
(p_alias VARCHAR2, p_fileName VARCHAR2) IS

CURSOR c_new_transactions IS
	SELECT *
	FROM new_transactions;

v_file 		UTL_FILE.FILE_TYPE;
v_aliasupper 	VARCHAR2(50);
v_row 			VARCHAR2(150);

BEGIN
	v_aliasupper := UPPER(p_alias);
	v_file := UTL_FILE.FOPEN(v_aliasupper, p_fileName, 'w', 32766);
	
	FOR r_new_transactions IN c_new_transactions LOOP
		
		v_row := r_new_transactions.transaction_no || ',' ||
				r_new_transactions.transaction_date || ',' ||
				r_new_transactions.description || ',' ||
				r_new_transactions.account_no || ',' ||
				r_new_transactions.transaction_type || ',' ||
				r_new_transactions.transaction_amount;
		
		UTL_FILE.PUT_LINE (v_file, v_row, true);
	
	END LOOP;
	UTL_FILE.FCLOSE(v_file);

END;
/

DECLARE

v_path 		VARCHAR2(50) := 'C:\CPRG_307A3';

BEGIN

proc_export_csv('CPRG307', 'output.txt');

END;
/