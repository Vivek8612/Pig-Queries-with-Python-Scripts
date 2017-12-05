#!/usr/bin/python
import ast

# @outputSchema("schema:chararray")
# @outputSchema('result:{t:(sid:chararray,accound_id:chararray)}')

@outputSchema('m:map[]')
def extract_account_info(account_details):
	
	#account_details = ast.literal_eval(account_details)	
	
	prod_name_list = ['AL', 'CC', 'CD', 'SA','COI', 'DDA', 'HIL', 'HLC', 'ILA', 'LOC', 'MLA', 'MMA']
	accnt_status_list = ['ACTIVE', 'EMPTY']
	
	product_details_dict = {}
	accont_status_dict = {}
	
	for item in prod_name_list:
		product_details_dict[item] = 0
	for item in accnt_status_list:
		accont_status_dict[item] = 0
	
	
	account_details_list = str(account_details).split("::")
	num_accounts = len(account_details_list)
	
	for rec in account_details_list:
		tmp_list = rec.split("|")
		
		product_identifier = tmp_list[-2]
		account_status = tmp_list[-1]
		if account_status == '':
			account_status = 'EMPTY'
		
		if product_identifier in product_details_dict.keys():
			product_details_dict[product_identifier] += 1
		
		if product_identifier == 'CC':
			if account_status in accont_status_dict.keys():
				accont_status_dict[account_status] += 1
	
	final_dict = {}
	final_dict['num_accounts'] = num_accounts
	final_dict['product_details'] = product_details_dict
	final_dict['account_status'] = accont_status_dict
		
	
	return final_dict