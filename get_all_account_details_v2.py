#!/usr/bin/python
import ast

# @outputSchema("schema:chararray")
# @outputSchema('result:{t:(sid:chararray,accound_id:chararray)}')

@outputSchema('m:map[]')
def extract_account_info(account_details, collec_cat_code_data):
	
	#account_details = ast.literal_eval(account_details)	
	
	#prod_name_list = ['AL', 'CC', 'CD', 'SA','COI', 'DDA', 'HIL', 'HLC', 'ILA', 'LOC', 'MLA', 'MMA']
	#accnt_status_list = ['ACTIVE', 'EMPTY']
	
	product_details_dict = {}
	account_status_dict = {}
	collection_dict = {}
	collection_flag = 1
	
	if str(collec_cat_code_data) == "NA" or str(collec_cat_code_data) == "":
		collection_flag = 0
		
	tmp_dict = {}
	if collection_flag == 1:
		cat_code_list = str(collec_cat_code_data).split("::")
		for item in cat_code_list:
			lst = item.split("|")
			tmp_dict[lst[0]] = lst[-1]
			
	
	account_details_list = str(account_details).split("::")
	num_accounts = len(account_details_list)
	
	num_collection_cards = 0
	
	for rec in account_details_list:
		tmp_list = rec.split("|")
		
		product_identifier = tmp_list[-2]
		account_status = tmp_list[-1]
		if account_status == '':
			account_status = 'EMPTY'
		
		if product_identifier in product_details_dict.keys():
			product_details_dict[product_identifier] += 1
		else:
			product_details_dict[product_identifier] = 1
		
		if product_identifier == 'CC':
			if account_status in account_status_dict.keys():
				account_status_dict[account_status] += 1
			else:
				account_status_dict[account_status] = 1
		
			if collection_flag == 1:
				prod_ref_id = tmp_list[0]
				if prod_ref_id in tmp_dict.keys():
					if tmp_dict[prod_ref_id] != "":
						num_collection_cards += 1
					
					
	if 'CC' not in product_details_dict.keys() or collection_flag == 0:
		collection_dict['num_collection_cards'] = 0
		collection_dict['collection_flag'] = 0
	else:
		if product_details_dict['CC'] < num_accounts:
			collection_dict['num_collection_cards'] = num_collection_cards
			collection_dict['collection_flag'] = 0
		else:
			collection_dict['num_collection_cards'] = num_collection_cards
			collection_dict['collection_flag'] = 1
	
	final_dict = {}
	final_dict['num_accounts'] = num_accounts
	final_dict['product_details'] = product_details_dict
	final_dict['account_status'] = account_status_dict
	final_dict['collection_details'] = collection_dict
		
	
	return final_dict