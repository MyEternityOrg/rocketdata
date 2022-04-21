select 
	guid,
	description,
	isnull(geopos_x, 0.000000),
	isnull(geopos_y, 0.000000),
	activity
from [system.divisiongroups]