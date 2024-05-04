create or replace package body      cdc_WKSHOP_mo_import_pkg as

/******************************************************************************
   NAME:       cdcif_ws_mo_import_pkg
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        09-09-2019   Alan Poon        1. Created this package body.
******************************************************************************/

--common variable

	l_statusNew        varchar2(1) := 'N'; --New Records
	l_statusError      varchar2(1) := 'E'; --Validate Error
	l_statusFailed     varchar2(1) := 'F'; --Import Failed
	l_statusDeleted    varchar2(1) := 'X'; --Deleted / Useless
	l_statusProcess    varchar2(1) := 'P'; --Validate Passed
	l_statusCompleted  varchar2(1) := 'C'; --Import Completed
	l_hdr_rec          apps.inv_move_order_pub.trohdr_rec_type := apps.inv_move_order_pub.g_miss_trohdr_rec;
	x_hdr_rec          apps.inv_move_order_pub.trohdr_rec_type := apps.inv_move_order_pub.g_miss_trohdr_rec;
	l_line_tbl         apps.inv_move_order_pub.trolin_tbl_type := apps.inv_move_order_pub.g_miss_trolin_tbl;
	x_line_tbl         apps.inv_move_order_pub.trolin_tbl_type := apps.inv_move_order_pub.g_miss_trolin_tbl;
	x_hdr_val_rec      apps.inv_move_order_pub.trohdr_val_rec_type;
	x_line_val_tbl     apps.inv_move_order_pub.trolin_val_tbl_type;
	v_msg_index_out    number;

--common cursor

	cursor mo_group (p_mo_import_request_id	number) is

		select
			cwmit.org
			, cwmit.from_sub_inv
			, cwmit.to_sub_inv
			, cwmit.need_by_date
			, cwmit.order_date
			, nvl(cwmit.mo_type,'NA') as mo_type
		from
			cdcif.cdcif_ws_mo_import_tab cwmit
		where
			cwmit.process_flag = l_statusProcess
			and cwmit.request_id = p_mo_import_request_id
		group by
			cwmit.org
			, cwmit.from_sub_inv
			, cwmit.to_sub_inv
			, cwmit.need_by_date
			, cwmit.order_date
			, nvl(cwmit.mo_type,'NA')
		order by
			cwmit.org
			, cwmit.from_sub_inv
			, cwmit.to_sub_inv
			, cwmit.need_by_date
			, cwmit.order_date;

--common cursor

	cursor prod_mo_header (p_mo_import_request_id	number
						   , p_org         			varchar2
						   , p_from_subinv 			varchar2
						   , p_to_subinv   			varchar2
						   , p_need_by_date			varchar2
						   , p_order_date  			varchar2) is

		select 
			cwmit.org
			, cwmit.bo_number
			, cwmit.to_sub_inv
			, nvl(cwmit.mo_type,'NA') as mo_type
			, cwmit.order_date
			, cwmit.order_remark
			, nvl(distribute_batch_name, 'NA') as distribute_batch_name
		from   
			cdcif.cdcif_ws_mo_import_tab cwmit
		where 
			cwmit.process_flag = l_statusProcess
			and cwmit.request_id = p_mo_import_request_id
			and cwmit.org = p_org
			and nvl(cwmit.from_sub_inv, 'null') = nvl(p_from_subinv, 'null')
			and cwmit.to_sub_inv = p_to_subinv
			and to_char(cwmit.need_by_date, 'YYYYMMDDHH24MISS') = p_need_by_date
			and to_char(cwmit.order_date, 'YYYYMMDDHH24MISS') = p_order_date
		group by
			cwmit.org
			, cwmit.bo_number
			, cwmit.to_sub_inv
			, nvl(cwmit.mo_type, 'NA')
			, nvl(cwmit.from_sub_inv, 'null')
			, cwmit.order_date
			, cwmit.order_remark
			, nvl(distribute_batch_name, 'NA')
		order by
			cwmit.to_sub_inv;

--common cursor

	cursor mo_header (p_mo_import_request_id	number
					  , p_org                   varchar2
					  , p_from_subinv           varchar2
					  , p_to_subinv             varchar2
					  , p_need_by_date          varchar2
					  , p_order_date            varchar2) is

		select
			cwmit.org
			, cwmit.bo_number
			, cwmit.to_sub_inv
			, nvl(cwmit.mo_type,'NA') as mo_type
			, cwmit.order_date
			, cwmit.order_remark
			, nvl(distribute_batch_name, 'NA') as distribute_batch_name
		from
			cdcif.cdcif_ws_mo_import_tab cwmit
		where
			cwmit.process_flag = l_statusProcess
			and cwmit.request_id = p_mo_import_request_id
			and cwmit.org = p_org
			and nvl(cwmit.from_sub_inv, 'null') = nvl(p_from_subinv, 'null')
			and cwmit.to_sub_inv = p_to_subinv
			and to_char(cwmit.need_by_date, 'ddmmyy') = p_need_by_date
			and to_char(cwmit.order_date, 'ddmmyy') = p_order_date
		group by
			cwmit.org
			, cwmit.bo_number
			, cwmit.to_sub_inv
			, nvl(cwmit.mo_type, 'NA')
			, nvl(cwmit.from_sub_inv, 'null')
			, cwmit.order_date
			, cwmit.order_remark
			, nvl(distribute_batch_name, 'NA')
		order by
			cwmit.to_sub_inv;

--common cursor

	cursor mo_details (p_mo_import_request_id	number
					   , p_org              		varchar2
					   , p_from_subinv      		varchar2
					   , p_to_subinv        		varchar2
					   , p_need_by_date     		varchar2
					   , p_order_date       		varchar2) is

		select
			*
		from
			cdcif.cdcif_ws_mo_import_tab cwmit
		where
			cwmit.process_flag = l_statusProcess
			and cwmit.request_id = p_mo_import_request_id
			and cwmit.org = p_org
			and nvl(cwmit.from_sub_inv, 'null') = nvl(p_from_subinv, 'null')
			and cwmit.to_sub_inv = p_to_subinv
			and to_char(cwmit.need_by_date, 'ddmmyy') = p_need_by_date
			and to_char(cwmit.order_date, 'ddmmyy') = p_order_date;

--common cursor

	cursor prod_mo_details (p_mo_import_request_id  number
							, p_org                   varchar2
							, p_from_subinv           varchar2
							, p_to_subinv             varchar2
							, p_need_by_date          varchar2
							, p_order_date            varchar2) is

		select
			*
		from
			cdcif.cdcif_ws_mo_import_tab cwmit
		where
			cwmit.process_flag = l_statusProcess
			and cwmit.request_id = p_mo_import_request_id
			and cwmit.org = p_org
			and nvl(cwmit.from_sub_inv, 'null') = nvl(p_from_subinv, 'null')
			and cwmit.to_sub_inv = p_to_subinv
			and to_char(cwmit.need_by_date, 'YYYYMMDDHH24MISS') = p_need_by_date
			and to_char(cwmit.order_date, 'YYYYMMDDHH24MISS') = p_order_date;
			
      
      cursor smw_print (p_org_id number) is
		select distinct i.from_sub_inv, i.need_by_period, i.need_by_date
          from cdcif.cdcif_ws_mo_import_tab i--, apps.mtl_secondary_inventories  s
          where substr(from_sub_inv,1,3) = 'SMW' and org <> 'SMW'
         -- and ((i.need_by_period in ('SPECIAL','PM') and i.need_by_date  = trunc(sysdate))
         -- or  (i.need_by_period in ('SPECIAL','AM') and i.need_by_date  = trunc(sysdate)+ 1))
          and NVL(email_sent,'N') <> 'Y'
          and trunc(creation_date) = trunc(sysdate)
         -- and i.request_id = p_request_id
          and i.process_flag = 'C';
          
       cursor smw_error  is
       select distinct i.to_sub_inv, i.bo_number
          from cdcif.cdcif_ws_mo_import_tab i
          where substr(from_sub_inv,1,3) = 'SMW' and org <> 'SMW'
          and NVL(email_sent,'N') <> 'Y'
          and trunc(creation_date) = trunc(sysdate)
          and i.process_flag = 'E'
          and error_message like '%Cut-off time was passed%';

--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++
function validate_mo (p_mo_import_request_id  in number) return varchar2

as

	l_ret_value  varchar2(3);
	l_item_type  varchar2(10);

begin

	 fnd_file.put_line(fnd_file.log, 'calling validate mo...');
	  
	l_ret_value := 'Y';
	
	 /*-- FOR CPC IMPLEMENTATION--
        update
        cdcif.cdcif_ws_mo_import_tab cwmit 
        set
        cwmit.org_id = 334
        where
        (cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
        and cwmit.request_id = p_mo_import_request_id
        and (cwmit.to_sub_inv in  ('CPC01','CPC02') or cwmit.from_sub_inv in  ('CPC01','CPC02'))
        and cwmit.org_id = 330;
        
        commit;*/

	--init value
	-- process_date, last_update_date, last_updated_by, error_message
	update 
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_date = sysdate
		, cwmit.last_update_date = sysdate
		, cwmit.last_updated_by = fnd_global.user_id
		, cwmit.error_message = ''
	where
		(cwmit.process_flag IS null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id;

	--org_id
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.org_id = (select organization_id from org_organization_definitions where organization_code = cwmit.org)
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.org_id is null;

	--inventory_item_id
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.inventory_item_id = (select inventory_item_id from mtl_system_items_b where organization_id = cwmit.org_id and segment1 = cwmit.item_no)
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.inventory_item_id is null;

	--item_no
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.item_no = (select segment1 from mtl_system_items_b where organization_id = cwmit.org_id and inventory_item_id = cwmit.inventory_item_id)
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.item_no is null;

	--to_locator
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.to_locator = (cwmit.org || '.' || cwmit.to_sub_inv || '.')
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.to_locator is null;

	--from_sub_inv
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.from_sub_inv = get_from_sub_inv(cwmit.org_id, cwmit.inventory_item_id, cwmit.mo_type)
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.from_sub_inv is null;
		
		
		
		
	-- deploy on 5 OCT
		
    update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.qty = round(qty,1)
    where 
        (cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and from_sub_inv like 'SMW%'
        and qty <> round(qty,1)
        and inventory_item_id in
        (select msi.inventory_item_id from 
         apps.mtl_system_items  msi, apps.FND_LOOKUP_VALUES_VL  v,
         apps.FND_LOOKUP_TYPES_VL   t  WHERE msi.organization_id = 332
         AND v.enabled_flag = 'Y'
         AND msi.attribute_category = 'SFG'
         AND msi.inventory_item_status_code = 'Active'
         AND t.lookup_type = 'CDC_SMW_PRODUCT_ROUND'
         AND t.lookup_type = v.lookup_type
         AND v.lookup_code = msi.segment1);
         
    update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.qty = round(qty,0)
    where 
        (cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and from_sub_inv like 'SMW%'
        and qty <> floor(qty)
        and inventory_item_id not in
        (select msi.inventory_item_id from 
         apps.mtl_system_items  msi, apps.FND_LOOKUP_VALUES_VL  v,
         apps.FND_LOOKUP_TYPES_VL   t  WHERE msi.organization_id = 332
         AND v.enabled_flag = 'Y'
         AND msi.attribute_category = 'SFG'
         AND msi.inventory_item_status_code = 'Active'
         AND t.lookup_type = 'CDC_SMW_PRODUCT_ROUND'
         AND t.lookup_type = v.lookup_type
         AND v.lookup_code = msi.segment1);
	
    /*
	update 
	(
	select  p.request_id, p.process_flag, p.error_message, i.primary_uom_code, p.uom, from_uom_code, to_uom_code, p.uom_rate, c.conversion_rate  
	from apps.mtl_system_items i, MTL_UOM_CLASS_CONVERSIONS c, cdcif.cdcif_ws_mo_import_tab p
    where i.organization_id = 334 and i.primary_uom_code <> p.uom 
    and from_uom_code = i.primary_uom_code and c.conversion_rate <> p.uom_rate
    and i.inventory_item_id = p.inventory_item_id
    and i.inventory_item_id = c.inventory_item_id
    and p.uom = c.to_uom_code
    and p.from_sub_inv = 'CPC02') a
	set
	a.process_flag = l_statusError
	, a.error_message = 'Conversion Rate not match with oracle setup, ' || a.error_message	
	where  a.request_id = p_mo_import_request_id
	and (a.process_flag is null or a.process_flag = l_statusNew);*/
	
	

	--error -> Org_id
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Missing Org ID, ' || cwmit.error_message
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.org_id is null;

	--error -> mo_type
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Missing From Sub Inventory, ' || cwmit.error_message
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.from_sub_inv is null
		and nvl(cwmit.mo_type, 'NA') not in ('BO', 'PROD');

	--error -> to_sub_inv
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Missing To Sub Inventory, ' || cwmit.error_message
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.to_sub_inv is null;


	--error -> to_locator
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Missing To Locator, ' || cwmit.error_message
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.to_locator is null;

	--error -> item_no
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Missing Item Number, ' || cwmit.error_message
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.item_no is null;

	--error -> qty
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Quantity must be positive number, ' || cwmit.error_message
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.qty is not null
		and cwmit.qty < 0;

	--error -> uom
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Missing UOM, ' || cwmit.error_message
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.uom is null;

	--validation
	--update inventory_item_id
	update 
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.inventory_item_id = cdcif_interface_pkg.get_Inventory_Item_Id(cwmit.org_id, cwmit.item_no)
	where
		cwmit.inventory_item_id is null
		and cwmit.request_id = p_mo_import_request_id;

	-- Error when failed to update inventory item id
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Item Information mistake, ' || cwmit.error_message
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.inventory_item_id is null;

	-- validate org id
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Invalid Organization, ' || cwmit.error_message
	where
		not exists (select null from mtl_parameters where cwmit.org_id = organization_id)
		and (cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.org_id is not null;

	-- validate item
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Invalid Item, ' || cwmit.error_message
	where
		not exists (select segment1 from mtl_system_items_b where segment1 = trim(cwmit.item_no))
		and (cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.item_no is not null;

	-- validate org id and item
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Invalid Org ID of Item, ' || cwmit.error_message
	where
		not exists (select segment1 from mtl_system_items_b where segment1 = trim(cwmit.item_no) and organization_id = trim(cwmit.org_id))
		and (cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.item_no is not null;

	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Invalid of FMD Item, ' || cwmit.error_message
	where
		not exists (select segment1 from mtl_system_items_b where segment1 = trim(cwmit.item_no) and organization_id in (select organization_id from org_organization_definitions where organization_code not in ('FMD', 'DNY')))
		and (cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.item_no is not null;

	-- validate from sub inv
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Invalid From Sub Inventory, ' || cwmit.error_message
	where
		-- Alan Poon 20190919 [Start]
		--not exists(select null from mtl_secondary_inventories msi, org_organization_definitions org where msi.organization_id = org.organization_id and  msi.secondary_inventory_name  = cwmit.from_sub_inv and org.organization_code in ('FMD', 'DNY'))
		-- Alan Poon 20190919 [End]
		not exists(select null from mtl_secondary_inventories msi, org_organization_definitions org where msi.organization_id = org.organization_id and  msi.secondary_inventory_name  = cwmit.from_sub_inv and org.organization_code not in ('FMD', 'DNY'))
		and (cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.from_sub_inv is not null;

	-- validate to sub inv
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError		
		, cwmit.error_message = 'Invalid To Sub Inventory, '  || cwmit.error_message
	where
		not exists (select null from mtl_secondary_inventories where secondary_inventory_name = cwmit.to_sub_inv and organization_id = cwmit.org_id)
		and (cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.to_sub_inv is not null;

	--validate uom
	update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Invalid UOM, '  || cwmit.error_message
	where
		cwmit.uom not in (select secondary_uom_code from cdcif.mtl_onhand_quantities_detail lot where lot.inventory_item_id = cwmit.inventory_item_id and lot.organization_id = cwmit.org_id and lot.lot_number = cwmit.lot)
		and (cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.uom is not null
		and cwmit.lot is not null;
		
		
    update
		cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Locator does not exist in BO_SG, '|| cwmit.error_message
	where
		(cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and cwmit.org is not null
		and cwmit.from_sub_inv is not null
		and cwmit.to_sub_inv is not null
		and not exists(select * from apps.mtl_item_locations where
		segment1 = cwmit.org and segment2 = cwmit.to_sub_inv and subinventory_code = 'BO_SG' 
		and status_id = 1 and organization_id in
		(select organization_id from apps.mtl_secondary_inventories where secondary_inventory_name = 
		cwmit.from_sub_inv)) ;
		
		
		update
		cdcif.cdcif_ws_mo_import_tab cwmit
	    set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Cut-off time was passed, '|| cwmit.error_message
		where
		(cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
        and from_sub_inv like 'SMW%'
        and need_by_date = trunc(sysdate)
        and need_by_period = 'AM';
        
        
        update
		cdcif.cdcif_ws_mo_import_tab cwmit
	    set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Cut-off time was passed, '|| cwmit.error_message
		where
		(cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id
		and (select to_number(substr(workshop_order_cutoff_time,1,2))
		from bmsrptro.mt_branch@CDC_BMS_RO where branch_code = cwmit.from_sub_inv)> 14
        and from_sub_inv like 'SMW%'
        and need_by_date = trunc(sysdate)
        and need_by_period = 'PM';
		
		
		
		
		
    update
        cdcif.cdcif_ws_mo_import_tab cwmit
	set
		cwmit.process_flag = l_statusError
		, cwmit.error_message = 'Duplicate Orders for same need by date and period, '|| cwmit.error_message
    where  ( cwmit.from_sub_inv, cwmit.org, cwmit.to_sub_inv, cwmit.need_by_date, cwmit.need_by_period, cwmit.item_no) in 
        (
        select  from_sub_inv, org, to_sub_inv, need_by_date, need_by_period, item_no
        from  cdcif.cdcif_ws_mo_import_tab
        group  by  from_sub_inv, org, to_sub_inv, need_by_date, need_by_period, item_no
        having count(*) > 1)
        and  cwmit.from_sub_inv like 'SMW%'
        and (cwmit.process_flag IS NULL OR cwmit.process_flag = l_statusNew)
		and  cwmit.request_id = p_mo_import_request_id
		and  cwmit.from_sub_inv is not null
		and  cwmit.org is not null
		and  cwmit.to_sub_inv is not null
		and  cwmit.need_by_date is not null
		and  cwmit.need_by_period is not null
		and  cwmit.item_no is not null;
		
		
		
		

	--passed validate
	update 
		cdcif.cdcif_ws_mo_import_tab cwmit
	set 
		cwmit.process_flag = l_statusProcess
	where
		(cwmit.process_flag is null or cwmit.process_flag = l_statusNew)
		and cwmit.request_id = p_mo_import_request_id;

	commit;

	return l_ret_value;

end validate_mo;

--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++

function get_from_sub_inv (p_org_id in number
						   , p_inventory_item_id in number
						   , p_mo_type in varchar2) return varchar2

as

	l_ret_value     varchar2(30);
	l_item_type     varchar2(30);
	l_product_type  varchar2(20);
	l_staging       varchar2(30);

begin

	l_ret_value := 'Y';

	select 
		attribute_category
		--, attribute28
	into
		l_item_type
		--, l_product_type
	from
		mtl_system_items_b item
	where
		item.organization_id = p_org_id
		and item.inventory_item_id = p_inventory_item_id;

	if (l_item_type = 'Raw Material') then
		l_ret_value := cdcif_interface_pkg.get_item_fmd_storage(p_org_id, p_inventory_item_id);
	elsif (l_item_type = 'Non-Food') then
		l_ret_value := cdcif_interface_pkg.get_item_fmd_storage(p_org_id, p_inventory_item_id);
	elsif (l_item_type = 'SFG') then
		l_ret_value := cdcif_interface_pkg.get_item_sfg_storage(p_org_id, p_inventory_item_id);
	else
		l_ret_value := null;
	end if;

	if (l_ret_value is not null) then
		begin
			if (p_mo_type = 'PROD') then
				select 
					attribute7 
				into
					l_staging
				from
					mtl_secondary_inventories
				where
					secondary_inventory_name = l_ret_value;
			else
				select
					attribute1
				into
					l_staging
				from
					mtl_secondary_inventories
				where
					secondary_inventory_name = l_ret_value;
			end if;
		exception 
			when no_data_found then
				l_staging := null;
			when others then
				l_staging := null;
		end;
	end if;

	if (l_staging is not null) then
		return l_staging;
	else
		if (l_ret_value is not null) then
			return l_ret_value;
		else
			if (l_product_type = 'Dry') then
				return '102_DRY_SG';
			else
				return 'ASRS_SG';
			end if;
		end if;
	end if;

end get_from_sub_inv;

--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++

function import_mo_prod (p_mo_import_request_id	in number
						 , p_org         		in varchar2
						 , p_from_subinv 		in varchar2
						 , p_to_subinv   		in varchar2
						 , p_need_by_date		in date
						 , p_order_date  		in date) return varchar2

as

	l_ret_value       		varchar2(3);
	l_update_mo       		varchar2(3);
	idx               		number := 1;
	x_return_status   		varchar2(1);
	x_msg_data        		varchar2(4000);
	x_msg_count       		number;
	l_code_combination_id	number;
	l_bo_num          		varchar2(100);
	l_org_id          		number;
	l_org_code        		varchar2(10);
	l_item_type       		varchar2(10);
	l_secondary_rate  		number;
	l_secondary_qty   		number;
	l_secondary_uom   		varchar2(10);
	l_header_id       		number;
	l_need_by_date    		varchar2(30);
	l_order_date      		varchar2(30);
	l_prodcut_type    		varchar2(10);
	l_trx_process     		varchar2(3);
	l_pri_process     		number;
	l_sec_process     		number;
	x_primary_qty     		number;
	x_primary_uom     		varchar2(10);
	x_secondary_qty   		number;
	x_secondary_uom   		varchar2(10);

begin

	fnd_file.put_line(fnd_file.log, 'calling import mo...');

	l_ret_value := 'Y';

	l_line_tbl.delete;
	x_line_tbl.delete;

	if p_from_subinv is null then

		select 
			organization_id 
		into 
			l_org_id
		from
			org_organization_definitions
		where
			organization_code = 'FMD';
	else
		select
			organization_id
		into
			l_org_id
		from
			mtl_secondary_inventories
		where
			secondary_inventory_name = p_from_subinv;
	end if;

	fnd_file.put_line(fnd_file.log, 'l_org_id > ' || l_org_id);

	fnd_file.put_line(fnd_file.log, 'Staring mo header import');

	l_need_by_date := to_char(p_need_by_date, 'YYYYMMDDHH24MISS');
	l_order_date := to_char(p_order_date, 'YYYYMMDDHH24MISS');

	fnd_file.put_line(fnd_file.log, 'Need By Date' ||  l_need_by_date);

	for mo_h in prod_mo_header (p_mo_import_request_id => p_mo_import_request_id
								, p_org                => p_org
								, p_from_subinv        => p_from_subinv
								, p_to_subinv          => p_to_subinv
								, p_need_by_date       => l_need_by_date
								, p_order_date         => l_order_date) loop

		fnd_file.put_line(fnd_file.log, 'Staring mo header > ' || 'Branch Move Order:' || p_org || '.' || p_to_subinv);

		if (mo_h.mo_type is not null AND mo_h.mo_type = 'PROD') then
			l_hdr_rec.date_required := p_need_by_date;
		else
			l_hdr_rec.date_required := to_date(l_need_by_date||' 13:00', 'ddmmyy HH24:mi');
		end if;

		l_hdr_rec.header_status      	:= inv_globals.g_to_status_incomplete;
		l_hdr_rec.organization_id    	:= l_org_id;
		l_hdr_rec.status_date        	:= sysdate;
		l_hdr_rec.transaction_type_id	:= inv_globals.g_type_transfer_order_subxfr;
		l_hdr_rec.move_order_type    	:= inv_globals.g_move_order_requisition;
		l_hdr_rec.db_flag            	:= fnd_api.g_true;
		l_hdr_rec.operation          	:= inv_globals.g_opr_create;		

		if p_from_subinv is not null then
			l_hdr_rec.from_subinventory_code := p_from_subinv;
		else
			l_hdr_rec.from_subinventory_code := null;
		end if;

		fnd_file.put_line(fnd_file.log,  'FROM SUBINV' || l_hdr_rec.from_subinventory_code);

		if (mo_h.mo_type is not null and mo_h.mo_type = 'QC') then
			l_hdr_rec.to_subinventory_code	:= mo_h.to_sub_inv;
			l_hdr_rec.description			:= 'QC Move Order: ' || p_org || '.' || p_to_subinv;			
		elsif (mo_h.mo_type is not null and mo_h.mo_type = 'BO') then
			l_hdr_rec.description           := 'Web ADI: ' || mo_h.ORDER_REMARK;
			l_hdr_rec.to_subinventory_code	:= 'BO_SG'; 
			l_hdr_rec.attribute1            := 'Branch';
			l_hdr_rec.attribute2            := mo_h.bo_number;
			l_hdr_rec.attribute3            := to_char(mo_h.order_date, 'RRRR/MM/dd') || ' 13:00:00';
		elsif (mo_h.mo_type is not null and mo_h.mo_type = 'PROD') then	
			l_hdr_rec.description           := 'Web ADI: ' || mo_h.ORDER_REMARK;
			l_hdr_rec.to_subinventory_code  := mo_h.to_sub_inv;
			l_hdr_rec.attribute1            := 'Production';		
		else
			l_hdr_rec.description           := 'Branch Move Order: ' || p_org || '.' || p_to_subinv;
			l_hdr_rec.to_subinventory_code  := 'BO_SG';
			l_hdr_rec.attribute1            := 'Branch';
			l_hdr_rec.attribute2            := mo_h.bo_number;
			l_hdr_rec.attribute3            := to_char(mo_h.order_date, 'RRRR/MM/dd') || ' 13:00:00';			
		end if;

		l_bo_num := mo_h.bo_number;

	end loop;

	idx := 1;

	fnd_file.put_line(fnd_file.log, 'Staring mo line import');

	for mo_d in prod_mo_details (p_mo_import_request_id	=> p_mo_import_request_id
								 , p_org                => p_org
								 , p_from_subinv        => p_from_subinv
								 , p_to_subinv          => p_to_subinv
								 , p_need_by_date       => l_need_by_date
								 , p_order_date         => l_order_date) loop

		fnd_file.put_line(fnd_file.log, 'Staring mo line get Primary Qty > ' || mo_d.org_id || ', ' || mo_d.inventory_item_id || ' Order qty: ' || mo_d.qty || ', ' || mo_d.uom);

		l_pri_process := cdcif_interface_pkg.get_primary_trx(mo_d.org_id, mo_d.inventory_item_id, null, mo_d.qty, mo_d.uom, x_primary_qty, x_primary_uom);
        
		fnd_file.put_line(fnd_file.log, 'Primary qty: ' || x_primary_qty||' '||x_primary_uom);

		
		l_sec_process := cdcif_interface_pkg.get_secondary_trx(mo_d.org_id, mo_d.inventory_item_id, null, mo_d.qty, mo_d.uom, x_secondary_qty, x_secondary_uom);	
        
		fnd_file.put_line(fnd_file.log, 'Secondary qty: ' || x_secondary_qty||' '||x_secondary_uom);
		
		
	
            select 
			nvl(attribute28, 'NA')
            into 
			l_prodcut_type
            from 
			mtl_system_items_b
            where 
			organization_id = l_org_id
			and inventory_item_id = mo_d.inventory_item_id;
        
        
        
		  select 
			organization_code into l_org_code
		    from 
			org_organization_definitions
		    where 
			organization_id = l_org_id;		
       
        
		if (mo_d.mo_type is not null and mo_d.mo_type = 'PROD') then
			l_line_tbl(idx).date_required := p_need_by_date;
		else
			l_line_tbl(idx).date_required := to_date(l_need_by_date || ' 13:00', 'ddmmyy HH24:mi');
		end if;

		l_line_tbl(idx).inventory_item_id       := mo_d.inventory_item_id;
		l_line_tbl(idx).line_id                 := fnd_api.g_miss_num;
		l_line_tbl(idx).line_number             := idx;
		l_line_tbl(idx).line_status             := inv_globals.g_to_status_incomplete;
		l_line_tbl(idx).transaction_type_id		:= inv_globals.g_type_transfer_order_subxfr;
		l_line_tbl(idx).organization_id         := l_org_id;
		l_line_tbl(idx).status_date             := sysdate;
		l_line_tbl(idx).quantity                := x_primary_qty; 
		l_line_tbl(idx).uom_code                := x_primary_uom;	

		if (x_secondary_uom is not null) then
			l_line_tbl(idx).secondary_quantity	:= x_secondary_qty;
			l_line_tbl(idx).secondary_uom     	:= x_secondary_uom;
		end if;

		l_line_tbl(idx).db_flag       			:= fnd_api.g_true;
		l_line_tbl(idx).operation     			:= inv_globals.g_opr_create;	

		if mo_d.from_sub_inv is not null then
			l_line_tbl(idx).from_subinventory_code	:= mo_d.from_sub_inv;
			l_line_tbl(idx).from_locator_id			:= mo_d.from_locator_id;
			fnd_file.put_line(fnd_file.log, 'From : ' || mo_d.from_sub_inv);
		end if;

		if mo_d.mo_type = 'QC' then
			l_line_tbl(idx).to_subinventory_code	:= mo_d.to_sub_inv;
			l_line_tbl(idx).to_locator_id          	:= cdcif_interface_pkg.get_locator_id(l_org_id, mo_d.to_sub_inv, mo_d.to_locator);
			fnd_file.put_line(fnd_file.log, 'QC: ' ||  l_org_id || ' ' || mo_d.to_sub_inv || ' ' || mo_d.to_locator);
		elsif mo_d.mo_type = 'PROD' then
			l_line_tbl(idx).to_subinventory_code	:= mo_d.to_sub_inv;
			l_line_tbl(idx).to_locator_id          	:= cdcif_interface_pkg.get_locator_id(l_org_id, mo_d.to_sub_inv, mo_d.to_locator);
			fnd_file.put_line(fnd_file.log, 'PROD: ' ||  l_org_id || ' ' || mo_d.to_sub_inv || ' ' || mo_d.to_locator);
		else
			l_line_tbl(idx).to_subinventory_code	:= 'BO_SG';
			fnd_file.put_line(fnd_file.log, 'TEST: ' || l_org_id || mo_d.to_locator); -- For check
			l_line_tbl(idx).to_locator_id			:= cdcif_interface_pkg.get_locator_id(l_org_id, 'BO_SG', mo_d.to_locator);
		end if;

		l_line_tbl(idx).lot_number	:= mo_d.lot;
		l_line_tbl(idx).attribute1	:= mo_d.bo_line_num;
		l_line_tbl(idx).attribute2	:= mo_d.qty;
		l_line_tbl(idx).attribute3	:= cdcif_interface_pkg.get_unit_of_measure(mo_d.uom);
		l_line_tbl(idx).attribute4	:= '' || mo_d.uom_rate;

		if (l_org_code = 'DNY') then
			l_line_tbl(idx).attribute12	:= mo_d.dny_route_code;
		else
			if (l_prodcut_type = 'Wet') then
				l_line_tbl(idx).attribute12 := mo_d.WET_ROUTE_CODE;
			elsif (l_prodcut_type = 'Dry') then 
				l_line_tbl(idx).attribute12 := mo_d.dry_route_code;
			else
				l_line_tbl(idx).attribute12 := mo_d.route_code;
			end if;
		end if;

		l_line_tbl(idx).attribute13	:= mo_d.need_by_period; -- Added by Alan Poon 20190919

		fnd_file.put_line(fnd_file.log, 'End Staring mo line > ' || mo_d.inventory_item_id);

		idx := idx + 1;

	end loop;

	fnd_file.put_line(fnd_file.log, '===================================');
	fnd_file.put_line(fnd_file.log, 'Calling INV_MOVE_ORDER_PUB to Create MO');

	inv_move_order_pub.process_move_order (p_api_version_number => 1.0
										   , p_init_msg_list 	=> fnd_api.g_false
										   , p_return_values 	=> fnd_api.g_false
										   , p_commit        	=> fnd_api.g_false
										   , x_return_status 	=> x_return_status
										   , x_msg_count     	=> x_msg_count
										   , x_msg_data      	=> x_msg_data
										   , p_trohdr_rec    	=> l_hdr_rec
										   , p_trolin_tbl    	=> l_line_tbl
										   , x_trohdr_rec    	=> x_hdr_rec
										   , x_trohdr_val_rec	=> x_hdr_val_rec
										   , x_trolin_tbl    	=> x_line_tbl
										   , x_trolin_val_tbl	=> x_line_val_tbl);

	fnd_file.put_line(fnd_file.log, 'Return Status: ' || x_return_status);

	fnd_file.put_line(fnd_file.log, 'Message Count: ' || x_msg_count);

	if x_return_status = 'S' then
		commit;
		fnd_file.put_line(fnd_file.log, 'Move Order Successfully Created');
		fnd_file.put_line(fnd_file.log, 'Move Order Number is :=> '|| x_hdr_rec.request_number);
		fnd_file.put_line(fnd_file.log, '===================================');	
	else
		rollback;
		fnd_file.put_line(fnd_file.log, 'Move Order Creation Failed Due to Following Reasons');
		fnd_file.put_line(fnd_file.log, '===================================');		
	end if;

	if x_msg_count > 0 then

		for v_index in 1 .. x_msg_count loop

			fnd_msg_pub.get (p_msg_index     	=> v_index
							 , p_encoded       	=> 'F'
							 , p_data          	=> x_msg_data
							 , p_msg_index_out	=> v_msg_index_out);

			x_msg_data := substr(x_msg_data, 1, 200);
			fnd_file.put_line(fnd_file.log, x_msg_data);

			update
				cdcif.cdcif_ws_mo_import_tab cwmit
			set
				cwmit.process_flag = l_statusError, cwmit.error_message = x_msg_data || ' ' || cwmit.error_message
			where
				(cwmit.process_flag is null or cwmit.process_flag = l_statusProcess)
				and cwmit.request_id = p_mo_import_request_id
				and cwmit.org = p_org
				and nvl(cwmit.from_sub_inv, 'NA') = nvl(p_from_subinv, 'NA')
				and cwmit.to_sub_inv = p_to_subinv
				and to_char(cwmit.need_by_date, 'YYYYMMDDHH24MISS') = l_need_by_date
				and to_char(cwmit.order_date, 'YYYYMMDDHH24MISS') = l_order_date;

		end loop;

	else

		update 
			cdcif.cdcif_ws_mo_import_tab cwmit
		set
			cwmit.process_flag = l_statusCompleted
			, cwmit.mo_number = x_hdr_rec.request_number
		where
			(cwmit.process_flag is null or cwmit.process_flag = l_statusProcess)
			and cwmit.request_id = p_mo_import_request_id
			and cwmit.org = p_org
			and nvl(cwmit.from_sub_inv, 'NA') = nvl(p_from_subinv, 'NA')
			and cwmit.to_sub_inv = p_to_subinv
			and to_char(cwmit.need_by_date, 'YYYYMMDDHH24MISS') = l_need_by_date
			and to_char(cwmit.order_date, 'YYYYMMDDHH24MISS') = l_order_date;

	end if;

	commit;

	return l_ret_value;

end import_mo_prod;

--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++			

function import_mo (p_mo_import_request_id	in number
					, p_org                   in varchar2
					, p_from_subinv           in varchar2
					, p_to_subinv             in varchar2
					, p_need_by_date          in date
					, p_order_date            in date) return varchar2

as 

	l_ret_value       		varchar2(3);
	l_update_mo       		varchar2(3);
	idx               		number := 1;
	x_return_status   		varchar2(1);
	x_msg_data        		varchar2(4000);
	x_msg_count       		number;
	l_code_combination_id	number;
	l_bo_num          		varchar2(100);
	l_org_id          		number;
	l_org_code        		varchar2(10);
	l_item_type       		varchar2(10);
	l_order_pri_qty   		number;
	l_pri_uom_code    		varchar2(10);
	l_secondary_rate  		number;
	l_secondary_qty   		number;
	l_secondary_uom   		varchar2(10);
	l_prodcut_type    		varchar2(20);
	l_header_id       		number;
	l_need_by_date    		varchar2(30);
	l_order_date      		varchar2(30);
	l_trx_process     		varchar2(3);
	l_pri_process     		number;
	l_sec_process     		number;
	x_primary_qty     		number;
	x_primary_uom     		varchar2(10);
	x_secondary_qty   		number;
	x_secondary_uom   		varchar2(10);

begin

	fnd_file.put_line(fnd_file.log, 'calling import mo...');

	l_ret_value := 'Y';

	l_line_tbl.delete;
	x_line_tbl.delete;

	if p_from_subinv is null then
		select 
			organization_id
		into 
			l_org_id
		from
			org_organization_definitions
		where
			organization_code = 'FMD';
    elsif p_from_subinv = 'CPC02' then
         
        l_org_id  := 334;
       
	else 
		select
			organization_id
		into
			l_org_id
		from
			mtl_secondary_inventories
		where
			secondary_inventory_name = p_from_subinv;
	end if;

	fnd_file.put_line(fnd_file.log, 'From SubInv > ' || p_from_subinv);

	fnd_file.put_line(fnd_file.log, 'ORG ID > ' || l_org_id);

	fnd_file.put_line(fnd_file.log, 'Staring mo header import');

	l_need_by_date := to_char(p_need_by_date, 'ddmmyy');

	l_order_date := to_char(p_order_date, 'ddmmyy');

	for mo_h in mo_header (p_mo_import_request_id	=> p_mo_import_request_id
						   , p_org                  	=> p_org
						   , p_from_subinv          	=> p_from_subinv
						   , p_to_subinv            	=> p_to_subinv
						   , p_need_by_date         	=> l_need_by_date
						   , p_order_date           	=> l_order_date) loop

		fnd_file.put_line(fnd_file.log, 'Staring mo header > ' || 'Branch Move Order:' || p_org || '.' || p_to_subinv);

		if (mo_h.mo_type is not null and mo_h.mo_type = 'PROD') then
			l_hdr_rec.date_required := p_need_by_date;
		else
			l_hdr_rec.date_required := to_date(l_need_by_date||' 13:00', 'ddmmyy HH24:mi');
		end if;

		l_hdr_rec.header_status   		:= inv_globals.g_to_status_incomplete;
		l_hdr_rec.organization_id 		:= l_org_id;
		l_hdr_rec.status_date     		:= sysdate;
		l_hdr_rec.transaction_type_id	:= inv_globals.g_type_transfer_order_subxfr;
		l_hdr_rec.move_order_type  		:= inv_globals.g_move_order_requisition;
		l_hdr_rec.db_flag          		:= fnd_api.g_true;
		l_hdr_rec.operation        		:= inv_globals.g_opr_create;

		if p_from_subinv is not null then
			l_hdr_rec.from_subinventory_code := p_from_subinv;
		else
			l_hdr_rec.from_subinventory_code := null;
		end if;

		fnd_file.put_line(fnd_file.log,  'FROM SUBINV' || l_hdr_rec.from_subinventory_code);

		if (mo_h.mo_type is not null and mo_h.mo_type = 'QC') then
			l_hdr_rec.to_subinventory_code   := mo_h.to_sub_inv;
			l_hdr_rec.description            := 'QC Move Order: ' || p_org || '.' || p_to_subinv;
		elsif (mo_h.mo_type is not null and mo_h.mo_type = 'BO') then
			l_hdr_rec.description         	:= 'Web ADI: ' || mo_h.order_remark;
			l_hdr_rec.to_subinventory_code	:= 'BO_SG';
			l_hdr_rec.attribute1          	:= 'Branch';
			l_hdr_rec.attribute2          	:= mo_h.bo_number;
			l_hdr_rec.attribute3          	:= to_char(mo_h.order_date, 'RRRR/MM/dd') || ' 13:00:00';			
		elsif (mo_h.mo_type is not null AND mo_h.mo_type = 'PROD') then
			l_hdr_rec.description         	:= 'Web ADI: ' || mo_h.order_remark;
			l_hdr_rec.to_subinventory_code	:= mo_h.to_sub_inv;
			l_hdr_rec.attribute1          	:= 'Production';
		else
			if (nvl(mo_h.distribute_batch_name, 'NA') = 'NA') then
				l_hdr_rec.description := 'Branch Move Order: ' || p_org || '.' || p_to_subinv;
			else
				l_hdr_rec.description := 'Distribute Move Order: ' || p_org || '.' || p_to_subinv || ' ' || mo_h.distribute_batch_name;
			end if;

			l_hdr_rec.to_subinventory_code	:= 'BO_SG';
			l_hdr_rec.attribute1          	:= 'Branch';
			l_hdr_rec.attribute2          	:= mo_h.bo_number;
			l_hdr_rec.attribute3          	:= to_char(mo_h.order_date, 'RRRR/MM/dd') || ' 13:00:00';		

		end if;

		l_bo_num := mo_h.bo_number;

	end loop;

	idx := 1;

	fnd_file.put_line(fnd_file.log, 'Staring mo line import');

	for mo_d in mo_details (p_mo_import_request_id	=> p_mo_import_request_id
							, p_org         		=> p_org
							, p_from_subinv 		=> p_from_subinv
							, p_to_subinv   		=> p_to_subinv
							, p_need_by_date		=> l_need_by_date
							, p_order_date  		=> l_order_date) loop

		fnd_file.put_line(fnd_file.log, 'Staring mo line get Primary Qty > ' || mo_d.org_id || ', ' || mo_d.inventory_item_id || ' Order qty: ' || mo_d.qty || ', ' || mo_d.uom);

		select
			primary_uom_code
			, attribute28
		into
			l_pri_uom_code
			, l_prodcut_type
		from
			mtl_system_items_b
		where
			organization_id = l_org_id
			and inventory_item_id =  mo_d.inventory_item_id;

		if (mo_d.uom_rate is not null) then

			l_order_pri_qty := mo_d.qty * mo_d.uom_rate;

			fnd_file.put_line(fnd_file.log, ' Order Primary Qty > ' || l_order_pri_qty || ', ' || l_pri_uom_code);

			l_pri_process := cdcif_interface_pkg.get_primary_trx(mo_d.org_id, mo_d.inventory_item_id, null, l_order_pri_qty, l_pri_uom_code, x_primary_qty, x_primary_uom);

			l_sec_process := cdcif_interface_pkg.get_secondary_trx(mo_d.org_id, mo_d.inventory_item_id, null, l_order_pri_qty, l_pri_uom_code, x_secondary_qty, x_secondary_uom);

		else

			l_pri_process := cdcif_interface_pkg.get_primary_trx(mo_d.org_id, mo_d.inventory_item_id, null, mo_d.qty, mo_d.uom, x_primary_qty, x_primary_uom);

			l_sec_process := cdcif_interface_pkg.get_secondary_trx(mo_d.org_id, mo_d.inventory_item_id, null, mo_d.qty, mo_d.uom, x_secondary_qty, x_secondary_uom);

		end if;

		fnd_file.put_line(fnd_file.log, 'Staring mo line > ' || mo_d.inventory_item_id || ' Primary qty: ' || x_primary_qty);

		select
			organization_code
		into
			l_org_code
		from
			org_organization_definitions
		where
			organization_id = l_org_id;

		if (mo_d.mo_type is not null and mo_d.mo_type = 'PROD') then	
			l_line_tbl(idx).date_required := p_need_by_date;
		else
			l_line_tbl(idx).date_required := to_date(l_need_by_date || ' 13:00', 'ddmmyy HH24:mi');
		end if;

		l_line_tbl(idx).inventory_item_id  	:= mo_d.inventory_item_id;
		l_line_tbl(idx).line_id            	:= fnd_api.g_miss_num;
		l_line_tbl(idx).line_number        	:= idx;
		l_line_tbl(idx).line_status        	:= inv_globals.g_to_status_incomplete;
		l_line_tbl(idx).transaction_type_id	:= inv_globals.g_type_transfer_order_subxfr;
		l_line_tbl(idx).organization_id    	:= l_org_id;
		l_line_tbl(idx).status_date        	:= sysdate;
		l_line_tbl(idx).quantity      		:= x_primary_qty;
		l_line_tbl(idx).uom_code      		:= x_primary_uom;

		if (x_secondary_uom is not null) then
			l_line_tbl(idx).secondary_quantity	:= x_secondary_qty;
			l_line_tbl(idx).secondary_uom     	:= x_secondary_uom;
		end if;

		l_line_tbl(idx).db_flag  	:= fnd_api.g_true;
		l_line_tbl(idx).operation	:= inv_globals.g_opr_create;

		if mo_d.from_sub_inv is not null then
			l_line_tbl(idx).from_subinventory_code	:= mo_d.from_sub_inv;
			l_line_tbl(idx).from_locator_id       	:= mo_d.from_locator_id;
			fnd_file.put_line(fnd_file.log, 'From : ' || mo_d.from_sub_inv);
		end if;

		fnd_file.put_line(fnd_file.log, 'To Locator : ' || mo_d.to_locator);

		if mo_d.mo_type = 'QC' then
			l_line_tbl(idx).to_subinventory_code	:= mo_d.to_sub_inv;
			l_line_tbl(idx).to_locator_id       	:= cdcif_interface_pkg.get_locator_id(l_org_id, mo_d.to_sub_inv, mo_d.to_locator);
			fnd_file.put_line(fnd_file.log, 'QC: ' ||  l_org_id || ' ' || mo_d.to_sub_inv || ' ' || mo_d.to_locator);
		elsif mo_d.mo_type = 'PROD' then
			l_line_tbl(idx).to_subinventory_code   := mo_d.TO_SUB_INV;
			l_line_tbl(idx).to_locator_id          := cdcif_interface_pkg.get_locator_id(l_org_id, mo_d.to_sub_inv, mo_d.to_locator);
			fnd_file.put_line(fnd_file.log, 'PROD: ' ||  l_org_id || ' ' || mo_d.to_sub_inv || ' ' || mo_d.to_locator);	
		else
			l_line_tbl(idx).to_subinventory_code   := 'BO_SG';
			fnd_file.put_line(fnd_file.log, 'TEST 20191022'); -- TEST
			fnd_file.put_line(fnd_file.log, 'TEST: ' ||  l_org_id || ' ' || mo_d.to_sub_inv || ' ' || mo_d.to_locator); -- TEST
			l_line_tbl(idx).to_locator_id          := cdcif_interface_pkg.get_locator_id(l_org_id, 'BO_SG', mo_d.to_locator);
			--l_line_tbl(idx).to_locator_id          := 15627;
			fnd_file.put_line(fnd_file.log, 'TEST: ' ||  l_org_id || ' ' || mo_d.to_sub_inv || ' ' || mo_d.to_locator);	-- TEST			
		end if;

		l_line_tbl(idx).lot_number	:= mo_d.lot;
		l_line_tbl(idx).attribute1	:= mo_d.bo_line_num;
		l_line_tbl(idx).attribute2	:= mo_d.qty;
		l_line_tbl(idx).attribute3	:= cdcif_interface_pkg.get_unit_of_measure(mo_d.uom);
		l_line_tbl(idx).attribute4	:= '' || mo_d.uom_rate;	

		if (l_org_code = 'DNY') then
			l_line_tbl(idx).attribute12	:= mo_d.dny_route_code;
		else
			--Alan Poon 20191125 [Start] 
			--if (l_prodcut_type = 'Wet') then
			--	l_line_tbl(idx).attribute12	:= mo_d.wet_route_code;
			--elsif (l_prodcut_type = 'Dry') then
			--	l_line_tbl(idx).attribute12	:= mo_d.dry_route_code;
			--else
			If l_org_code = 'SMW' then
			 
                l_line_tbl(idx).attribute12	:= 'N/A';--mo_d.route_code; -- amended on 22 Dec
			else
			    l_line_tbl(idx).attribute12	:=  mo_d.route_code; -- amended on 22 Dec
			    
			end if;
			--Alan Poon 20191125 [End] 
		end if;

		l_line_tbl(idx).attribute13	:= mo_d.need_by_period; -- Added by Alan Poon 20190919

		fnd_file.put_line(fnd_file.log, 'End Staring mo line > ' || mo_d.inventory_item_id);

		idx := idx + 1;

	end loop;

	fnd_file.put_line(fnd_file.log, '===================================');
	fnd_file.put_line(fnd_file.log, 'Calling INV_MOVE_ORDER_PUB to Create MO');

	inv_move_order_pub.process_move_order (p_api_version_number	=> 1.0
										   , p_init_msg_list      => fnd_api.g_false
										   , p_return_values      => fnd_api.g_false
										   , p_commit             => fnd_api.g_false
										   , x_return_status      => x_return_status
										   , x_msg_count          => x_msg_count
										   , x_msg_data           => x_msg_data
										   , p_trohdr_rec         => l_hdr_rec
										   , p_trolin_tbl         => l_line_tbl
										   , x_trohdr_rec         => x_hdr_rec
										   , x_trohdr_val_rec     => x_hdr_val_rec
										   , x_trolin_tbl         => x_line_tbl
										   , x_trolin_val_tbl     => x_line_val_tbl);

	fnd_file.put_line(fnd_file.log, 'Return Status: ' || x_return_status);
	fnd_file.put_line(fnd_file.log, 'Message Count: ' || x_msg_count);

	if x_return_status = 'S' then
		commit;
		fnd_file.put_line(fnd_file.log, 'Move Order Successfully Created');
		fnd_file.put_line(fnd_file.log, 'Move Order Number is :=> '|| x_hdr_rec.request_number);
		fnd_file.put_line(fnd_file.log, '===================================');
		x_msg_count := null;
	else
		rollback;
		fnd_file.put_line(fnd_file.log, 'Move Order Creation Failed Due to Following Reasons');
		fnd_file.put_line(fnd_file.log, '===================================');
	end if;

	if x_msg_count > 0 then

		for v_index in 1 .. x_msg_count loop
			fnd_msg_pub.get (p_msg_index     	=> v_index
							 , p_encoded       	=> 'F'
							 , p_data          	=> x_msg_data
							 , p_msg_index_out	=> v_msg_index_out);

			x_msg_data := substr(x_msg_data, 1, 200);

			fnd_file.put_line(fnd_file.log, x_msg_data);

			update
				cdcif.cdcif_ws_mo_import_tab cwmit
			set
				cwmit.process_flag = l_statusError, cwmit.error_message = x_msg_data || ' ' || cwmit.error_message
			where
				(cwmit.process_flag is null or cwmit.process_flag = l_statusProcess)
				and cwmit.request_id = p_mo_import_request_id
				and cwmit.org = p_org
				and nvl(cwmit.from_sub_inv, 'NA') = nvl(p_from_subinv, 'NA')
				and cwmit.to_sub_inv = p_to_subinv
				and to_char(cwmit.need_by_date, 'ddmmyy') = l_need_by_date
				and to_char(cwmit.order_date, 'ddmmyy') = l_order_date;

		end loop;

	else

		update
			cdcif.cdcif_ws_mo_import_tab cwmit
		set
			cwmit.process_flag = l_statusCompleted
			, cwmit.mo_number = x_hdr_rec.request_number
		where
			(cwmit.process_flag is null or cwmit.process_flag = l_statusProcess)
			and cwmit.request_id = p_mo_import_request_id
			and cwmit.org = p_org
			and nvl(cwmit.from_sub_inv, 'NA') = nvl(p_from_subinv, 'NA')
			and cwmit.to_sub_inv = p_to_subinv
			and to_char(cwmit.need_by_date, 'ddmmyy') = l_need_by_date
			and to_char(cwmit.order_date, 'ddmmyy') = l_order_date;

	end if;

	commit;

	return l_ret_value;

end import_mo;	

--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++--++
--CDC Branch Order To Move Order (WorkShop)

procedure import_mo_process (errbuf out varchar2
							 , retcode out number)

is

	c_mo_import_request_id  constant number     := fnd_global.conc_request_id;
	c_errbuf_max            constant number(15) := 240;
	l_validate  			varchar2(3);
	l_update    			varchar2(3);
	l_import    			varchar2(3);
	l_result                number;
	l_t_phase               varchar2(100);     
	l_t_status              varchar2(100);  
    l_t_dev_phase           varchar2(100);
    l_t_dev_status          varchar2(100);
    l_t_message             varchar2(100);
    l_t_bool                  BOOLEAN;
    v_errbuf                varchar2(100);
    v_retcode               number;

begin

	fnd_file.put_line(fnd_file.log, '## STARTING CDCIF WS MO IMPORT ## ');

	fnd_file.put_line(fnd_file.log, to_char(sysdate, '>>> dd/mm/yyyy hh24:mi:ss >> ') ||'#01 Begin');

	fnd_file.put_line(fnd_file.log, to_char(sysdate, '>>> dd/mm/yyyy hh24:mi:ss >> ') ||'#02 Start [Assign Request ID.]');

	update 
		cdcif.cdcif_ws_mo_import_tab cwmit
	set  
		cwmit.request_id = c_mo_import_request_id
	where
		cwmit.request_id is null
		and (cwmit.process_flag is null or cwmit.process_flag = l_statusNew);

	commit;

	fnd_file.put_line(fnd_file.log, to_char(sysdate, '>>> dd/mm/yyyy hh24:mi:ss >> ') ||'#03 Start [Validate MO]');

	l_validate := validate_mo (p_mo_import_request_id => c_mo_import_request_id);

	fnd_file.put_line(fnd_file.log, to_char(sysdate, '>>> dd/mm/yyyy hh24:mi:ss >> ') ||'#04 Start [Import MO]');

	for mo in mo_group (p_mo_import_request_id => c_mo_import_request_id) loop

		fnd_file.put_line(fnd_file.log, 'Working on Move Order ORG: ' || mo.org || ', From SubInv: ' || mo.from_sub_inv || ', To SubInv: ' || mo.to_sub_inv || ', Date: ' || to_char(mo.need_by_date, 'ddmmyy'));

		fnd_file.put_line(fnd_file.log, mo.need_by_date || ' ' || mo.order_date);

		if mo.mo_type = 'PROD' then

			fnd_file.put_line(fnd_file.log, 'PROD MO import');
			fnd_file.put_line(fnd_file.log, 'From Subinv' || mo.from_sub_inv);

			l_import := import_mo_prod (p_mo_import_request_id => c_mo_import_request_id
										, p_org                  => mo.org
										, p_from_subinv          => mo.from_sub_inv
										, p_to_subinv            => mo.to_sub_inv
										, p_need_by_date         => mo.need_by_date
										, p_order_date           => mo.order_date);

		else

			l_import := import_mo (p_mo_import_request_id => c_mo_import_request_id
								   , p_org                  => mo.org
								   , p_from_subinv          => mo.from_sub_inv
								   , p_to_subinv            => mo.to_sub_inv
								   , p_need_by_date         => mo.need_by_date
								   , p_order_date           => mo.order_date);			

		end if;

	end loop;

	fnd_file.put_line(fnd_file.log, '## END OF CDCIF MO IMPORT ##');

	fnd_file.put_line(fnd_file.log, to_char(sysdate, '>>> dd/mm/yyyy hh24:mi:ss >> ') ||'#05 Completed.');
	/*
	IF c_mo_import_request_id > 0 THEN                             
                    
                            LOOP
                         
                            l_t_bool := fnd_concurrent.wait_for_request(c_mo_import_request_id,
                                                2, -- check every 2 seconds if the concurrent is finished
                                                0,
                                                l_t_phase,
                                                l_t_status,
                                                l_t_dev_phase,
                                                l_t_dev_status,
                                                l_t_message);     
                                                
                            commit;
                 				
      
                            EXIT
                            WHEN UPPER (l_t_phase) = 'COMPLETED' OR UPPER (l_t_status) IN ('CANCELLED', 'ERROR', 'TERMINATED');
                            END LOOP;     
                         
	 End If;
	 
	 begin
         select count(*) into l_result 
          from cdcif.cdcif_ws_mo_import_tab i--, apps.mtl_secondary_inventories  s
          where substr(from_sub_inv,1,3) = 'SMW' and org <> 'SMW'
         -- and ((i.need_by_period in ('SPECIAL','PM') and i.need_by_date  = trunc(sysdate))
         -- or  (i.need_by_period in ('SPECIAL','AM') and i.need_by_date  = trunc(sysdate)+ 1))
          and NVL(email_sent,'N') <> 'Y'
          and trunc(creation_date) = trunc(sysdate)
         -- and i.request_id = p_request_id
          and i.process_flag = 'C';
     exception when others then
          l_result := 0;
     End;
     
	 If l_result > 0 then
        submit_smw_report (v_errbuf, v_retcode); 
	 End If;*/

exception when others then

	rollback;
	retcode := '2';
	errbuf  := substr('Move Order Import Failed. '||sqlerrm, 1, c_errbuf_max);
	fnd_file.put_line(fnd_file.log, 'Move Order Import Failed.');
	fnd_file.put_line(fnd_file.log, sqlerrm);
	fnd_file.put_line(fnd_file.log, '');

end import_mo_process;


procedure submit_smw_report (errfuf out varchar2
                            , retcode out number)

is

 l_workshop        varchar2(50);
 default_printer    varchar2(50);
 default_email          varchar2(100);
 l_order_type         varchar2(20);
 l_date_req          Date;
 l_date_par         varchar2(20);
  l_boolean     boolean := TRUE;
   e_add_del     exception;
  e_add_layout  exception;
 e_set_option  exception;
 e_set_repeat  exception;
  l_conc_id      number;
  branch_email  varchar2(100);
  l_bo_number varchar2(30);

cursor smw_print  is
/*
select distinct i.from_sub_inv, i.need_by_period, i.need_by_date
from cdcif.cdcif_ws_mo_import_tab i--, apps.mtl_secondary_inventories  s
where substr(from_sub_inv,1,3) = 'SMW' and org <> 'SMW'
and NVL(email_sent,'N') <> 'Y'
and trunc(creation_date) = trunc(sysdate)
and i.process_flag = 'C';*/
select distinct i.from_sub_inv, i.need_by_period, i.need_by_date
from cdcif.cdcif_ws_mo_import_tab i--, apps.mtl_secondary_inventories  s
where substr(from_sub_inv,1,3) = 'SMW' and org <> 'SMW'
and ((need_by_date = trunc(sysdate) and need_by_period = 'PM') or (need_by_date = trunc(sysdate) + 1 and need_by_period = 'AM')
--or (need_by_date = trunc(sysdate) and need_by_period = 'SPECIAL'))
or ( (need_by_date between trunc(sysdate) and trunc(sysdate) + 10 )  and need_by_period = 'SPECIAL'))
and i.process_flag = 'C'
and NVL(i.email_sent,'N') = 'P';

begin

    begin
        update cdcif.cdcif_ws_mo_import_tab 
        set email_sent = 'P'
        where substr(from_sub_inv,1,3) = 'SMW' and org <> 'SMW'
        and ((need_by_date = trunc(sysdate) and need_by_period = 'PM') or (need_by_date = trunc(sysdate) + 1 and need_by_period = 'AM')
       -- or (need_by_date = trunc(sysdate) and need_by_period = 'SPECIAL'))
		or ( (need_by_date between trunc(sysdate) and trunc(sysdate) + 10 )  and need_by_period = 'SPECIAL'))
        and NVL(email_sent,'N') <> 'Y'
        and process_flag = 'C';
        
        update cdcif.cdcif_ws_mo_import_tab 
        set email_sent = 'P'
        where substr(from_sub_inv,1,3) = 'SMW' and org <> 'SMW'
       -- and ((need_by_date = trunc(sysdate) and need_by_period = 'PM') or (need_by_date = trunc(sysdate) + 1 and need_by_period = 'AM')
       -- or (need_by_date = trunc(sysdate) and need_by_period = 'SPECIAL'))
        and NVL(email_sent,'N') <> 'Y'
        and error_message like '%Cut-off time was passed%' 
        and process_flag = 'E';
        
        commit;
        
    exception when others then
    
        fnd_file.put_line(fnd_file.log,'Error when update email_sent to P');
    end;
    
     
    FOR c_smw in smw_print
    
    Loop
    
        l_workshop      := c_smw.from_sub_inv;
       
         fnd_file.put_line(fnd_file.log,'Workshop '||l_workshop);
             
        l_order_type   := c_smw.need_by_period;
       
        fnd_file.put_line(fnd_file.log,'Order Type '||l_order_type  );
       
        l_date_req     := c_smw.need_by_date;
       
        fnd_file.put_line(fnd_file.log,'Date Req '||l_date_req   );
       
        l_date_par     :=  to_char(l_date_req,'YYYYMMDD');
       
        fnd_file.put_line(fnd_file.log,'Date Parameter '||l_date_par);
       
       begin
          select -- s.attribute9, 
          s.attribute10 
          into --default_printer, 
          default_email
          from  apps.mtl_secondary_inventories  s
          where s.organization_id = 332
          and s.secondary_inventory_name = l_workshop;
          
          fnd_file.put_line(fnd_file.log,'Default Printer'||default_printer);
          
          fnd_file.put_line(fnd_file.log,'Default Email '||default_email);
          
       exception
          when no_data_found then
          default_printer := 'noprint';
          default_email   := 'fannylee@cafedecoral.com';
       end;
         
       /*
       l_boolean :=  fnd_request.set_print_options
                  (printer => default_printer,
                   style   => 'A4',	
                   copies => 1,
                   save_output  => TRUE,
                   print_together  => 'N');*/
                   
    /*
      if l_boolean then
    
       l_boolean := fnd_request.add_printer
                 (printer => default_printer,
                 copies => 0);
     
      end if;*/

    
      l_boolean := fnd_request.add_delivery_option
                    (type => 'E' -- this one to speciy the delivery option as email
                    ,p_argument1 => 'CDC 燒味工場柯打磅碼表 - 磅前'||'-'||l_workshop ||'-'||l_date_par||'-'||l_order_type  -- subject for the mail
                    ,p_argument2 =>  'EBSR12_HKEP1@cafedecoral.com' -- from address
                    ,p_argument3 =>  default_email  -- to address
                    --,p_argument4 => 'fannylee@cafedecoral.com' -- cc address to be specified here.
					,p_argument4 => 'davidleung@cafedecoral.com' -- cc address to be specified here.
                    ,nls_language => ''); -- Optional

       -- Adding Template to the request
    
    if l_boolean then
       
       
        l_boolean  :=   fnd_request.add_layout (
                        template_appl_name => 'CDC'
                       , template_code => 'SMWINTRAN-S'
                       , template_language  => 'en'
                       , template_territory  => 'US'
                       , output_format  => 'PDF'
                       );
               
                      
    else
       raise e_add_del;                                             
    end if; 

   
    if l_boolean then
       l_boolean := fnd_request.set_options ('YES');
    else
       raise e_add_layout;
    end if;

   
 
   

    if l_boolean then

        
                          
          l_conc_id  :=  apps.fnd_request.submit_request
                        (application => 'CDC',
                         program     => 'SMWINTRAN-S',
                         description => 'SMW Before-Weight Report',
                         argument1   => 332,
                         argument2   => l_date_par,
                         argument3   => l_workshop,
                         argument4   => l_order_type
                         );
                 

  else
       raise e_set_repeat;                                             
    end if;
   
    if l_conc_id > 0 then
       dbms_output.put_line('Concurrent Program Id: '||l_conc_id);
       
        begin
       update cdcif.cdcif_ws_mo_import_tab
       set email_sent = 'Y'
       where from_sub_inv = l_workshop 
       and need_by_period = l_order_type 
       and need_by_date = l_date_req  
       and substr(from_sub_inv,1,3) = 'SMW' 
       and org <> 'SMW'
       and email_sent = 'P'
       and process_flag = 'C';
              
       commit;
       
        exception when others then
       dbms_output.put_line('Concurrent Program Id: '||l_conc_id);
        end;   
    
    else
       dbms_output.put_line('Error: submit_request');
    end if;
    commit;
    
   
    
    End Loop;
    
    FOR c_error in smw_error
    
    Loop
      
        begin
            select distinct b.email into branch_email
            from 
            bmsrptro.mt_branch_outlet@CDC_BMS_RO o, bmsrptro.mt_branch@CDC_BMS_RO b, 
            bmsrptro.Mt_Sub_Zone@CDC_BMS_RO sz, bmsrptro.mt_zone@CDC_BMS_RO z, bmsrptro.mt_chain@CDC_BMS_RO ch 
            where o.branch_id = b.branch_id
            and sz.zone_id = z.zone_id
            and z.chain_id = ch.chain_id
            and ch.chain_code in ('SMW')
            and o.outlet_code = c_error.to_sub_inv;
        exception when others then
        
            branch_email := 'fannylee@cafedecoral.com';
        end;   
        
        l_bo_number := c_error.bo_number;
            
        fnd_file.put_line(fnd_file.log,'Branch Email '||branch_email);
          /*
         BEGIN
            UTL_MAIL.send(sender     => 'EBSR12_HKEP1@cafedecoral.com',
                recipients =>  branch_email ,
                cc         => 'fannylee@cafedecoral.com',
                bcc        => 'fannylee@cafedecoral.com',
                subject    => '工場訂單#'||l_bo_number,
                message    => '貴分店於BMS輸入的工場訂單#'||l_bo_number||',因已過了截單時間, 已被系統自動拒絕接受,如有須要, 請自行通知工場以加單形式進行, 謝謝');
         END;*/
    
         l_boolean := fnd_request.add_delivery_option
                    (type => 'E' -- this one to speciy the delivery option as email
                    ,p_argument1 => '貴分店於BMS輸入的工場訂單#'||l_bo_number||',因已過了截單時間, 已被系統自動拒絕接受'
                    ,p_argument2 =>  'EBSR12_HKEP1@cafedecoral.com' -- from address
                    ,p_argument3 =>  branch_email  -- to address
                    ,p_argument4 => 'fannylee@cafedecoral.com' -- cc address to be specified here.
                    ,nls_language => ''); -- Optional

     
       
        begin
            update cdcif.cdcif_ws_mo_import_tab
            set email_sent = 'Y'
            where to_sub_inv = c_error.to_sub_inv 
            and substr(from_sub_inv,1,3) = 'SMW' 
            and org <> 'SMW'
            and email_sent = 'P'
            and process_flag = 'E';
              
            commit;
        exception when others then
           dbms_output.put_line('Fail to update email sent flag');
        end; 
      
    
    
   
    
    End Loop;
    
    
 exception
    when e_add_del then
       dbms_output.put_line('Error: add_delivery_option');
    when e_add_layout then
       dbms_output.put_line('Error: add_layout');
    when e_set_option then
       dbms_output.put_line('Error: set_options');
    when e_set_repeat then
       dbms_output.put_line('Error: set_repeat_options' ); 
    when others then
       dbms_output.put_line('Error: '||sqlerrm);   



end submit_smw_report;

procedure smw_close_mo (errfuf out varchar2
                            , retcode out number)

is



begin

    begin
        update apps.mtl_txn_request_headers 
        set header_status = 3
        where organization_id = 332
        and header_id in 
        (select t.header_id from
        (select header_id, count(*) as count
        from apps.mtl_txn_request_lines where organization_id = 332 
        and line_status = 5 and attribute8 is not null and quantity_delivered is not null
        and trunc(date_required) = trunc(sysdate)
        group by header_id) c,
        (select header_id, count(*) as count
        from apps.mtl_txn_request_lines where organization_id = 332 
        and line_status <> 6 
        and trunc(date_required) = trunc(sysdate)
        group by header_id) t
        where c.header_id = t.header_id
        and c.count >= t.count)
        and organization_id = 332 
        and header_status = 1;
       -- and ((trunc(creation_date) = trunc(sysdate)) or (trunc(creation_date) = trunc(sysdate)-1));
        
        commit;
        
    exception when others then
    
        fnd_file.put_line(fnd_file.log,'Error when update header status');
    end;
    
    begin
    
        update apps.mtl_txn_request_headers 
        set header_status = 3
        where organization_id = 332
        and header_status = 1
        and header_id in
        (select header_id 
        from apps.mtl_txn_request_lines where organization_id = 332 
        and line_status = 5 and attribute8 is not null and quantity_delivered is not null
        and trunc(date_required) <= trunc(sysdate)
        )
       -- and ((trunc(creation_date) = trunc(sysdate)) or (trunc(creation_date) = trunc(sysdate)-1))
        and TO_CHAR(CURRENT_DATE, 'HH24:MI:SS') >= '21:00:00';
    
        commit;
    
    exception when others then
    
        fnd_file.put_line(fnd_file.log,'Error when update header status at 23:00');
    end;
    
    begin
     
        update apps.mtl_txn_request_lines 
        set attribute8 = substr(attribute8,1,instr(attribute8,attribute13)-1)||attribute13||'01'||decode(attribute12,'SYS','A','')
        where organization_id = 332 
        and line_status = 5 and attribute8 is not null and quantity_delivered is not null
        and to_char(trunc(date_required),'Mon-yy') = to_char(trunc(sysdate),'Mon-yy')
        and attribute8 IN (SELECT DISTINCT tab.mo_number
        FROM cdcif.cdcif_ws_mo_receive_tab tab
        WHERE (tab.process_flag IS NULL OR tab.process_flag IN ('N', 'P'))
        AND tab.mo_number IS NOT NULL)
        and line_id not in 
        (select mo_line_id from bmsprd.ot_receipt_bo_dtl@CDC_BMS r,
        bmsprd.OT_RECEIPT_HDR@CDC_BMS H
        where H.receipt_type = 'WO'
        and H.receipt_id = r.receipt_id
        and h.workshop_outlet_id in (select outlet_id from bmsprd.mt_branch_outlet@CDC_BMS where outlet_code like 'SMW%')
        and to_char(h.delivery_date,'Mon-yy')= to_char(trunc(sysdate),'Mon-yy')
        );
        
        commit;
    
    exception when others then
    
        fnd_file.put_line(fnd_file.log,'update completed');
    end;
     
  
end smw_close_mo ;

procedure smw_all_close_mo (errfuf out varchar2
                            , retcode out number)

is



begin

       
     
    begin
    
        update apps.mtl_txn_request_headers 
        set header_status = 3
        where organization_id = 332
        and header_status = 1
        and header_id in
        (select header_id 
        from apps.mtl_txn_request_lines where organization_id = 332 
        and line_status = 5 and attribute8 is not null and quantity_delivered is not null
        and trunc(date_required) = trunc(sysdate));
       -- and ((trunc(creation_date) = trunc(sysdate)) or (trunc(creation_date) = trunc(sysdate)-1))
       -- and TO_CHAR(CURRENT_DATE, 'HH24:MI:SS') >= '23:00:00';
    
        commit;
    
    exception when others then
    
        fnd_file.put_line(fnd_file.log,'Error when update header status at 23:00');
    end;

end smw_all_close_mo ;

end cdc_WKSHOP_mo_import_pkg;
