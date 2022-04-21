select * from 
(
select
	convert(int, _fld6648) as se_code,
     _Fld38720 as se_json_addr
from _Reference202_VT5339 as addr
left join _Reference251 as store on addr.[_Reference202_IDRRef] = store._fld6646rref
where 
	len(_fld6648) > 0 
	and right(_fld6648, 1) != '_'
) as r
group by
	se_code,
	se_json_addr