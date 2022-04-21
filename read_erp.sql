select * from 
(
select
	sprt_se._code as code,
	case(len(sprt_dv._description)) 
		when 11 then left(sprt_dv._description, 9) + '0' + right(sprt_dv._description, 2)
		else sprt_dv._description
	end as division,
	case len(contacts._fld6106)
		when 0 then '0'
		else contacts._fld6106
	end as postalcode,
	contacts._fld6107 as region,
	case 
		when len(contacts._fld6109) = 0 then contacts._fld6110
		else contacts._fld6109
	end as city,
	contacts._fld6111 as street,
	contacts._fld6112 as housenumber
from _reference145 as sprt_dv left join _inforg6101 as contacts on sprt_dv._idrref = contacts._fld6102_rrref
left join _reference78 as contacts_type on contacts._fld6104_rrref = contacts_type._idrref
left join _inforg7485 as link on sprt_dv._idrref = link._fld7488rref
left join _reference182 as sprt_se on link._fld7487rref = sprt_se._idrref
where
	--sprt_se._Marked = 0x0
	sprt_se._code between 51 and 997
	and contacts_type._code = 00011
	and sprt_dv._idrref not in
		(0x929be41f13e1089811e455216fdcd282)
) as x
group by
	code, 
	division,
	postalcode,
	region,
	city,
	street,
	housenumber