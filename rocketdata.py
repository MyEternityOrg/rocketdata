# Main script
import json

from data_connector import MSSQLConnection


def write_packet(brand):
    sql = MSSQLConnection()
    q = sql.select("exec rocketdata_export %s", (brand,))

    packet = []
    with open('addr_hash.json', 'r', encoding='utf-8') as d:
        addr = json.loads(d.read())

    for i in q:
        packet_row = {}
        packet_row['brand'] = brand
        packet_row['name_en'] = 'pokupochka'
        packet_row['code'] = str(i[0])
        packet_row['rocketdata_categories'] = [8107, 7852, 7002, 6982]
        packet_row['brand_alt_names'] = ['ООО Тамерлан']
        packet_row['alt_names'] = [i[3]]
        packet_row['address'] = addr.get(i[3])['address']
        # packet_row['address'] = {"lat": i[8], "lon": i[7], "country": "RU", "postalcode": i[5],
        #                          "address_language": "ru", "description": i[4]}
        packet_row['website'] = "https://pokupochka.ru/"
        packet_row['email'] = "info@pokupochka.ru"
        packet_row['phones'] = [{"is_main": True, "phone": "+78002013612"}]

        wtime = ''
        working_hours = {}

        if i[10] == i[11]:
            wtime = f'00:00-23:59'
        else:
            wtime = f'{i[10]}-{i[11]}'

        for x in range(1, 8):
            working_hours[f'{x}'] = [wtime]

        packet_row['details'] = {"working_hours": working_hours}

        if i[9] == 1:
            packet_row['closed'] = False
            packet_row['active'] = True
        else:
            packet_row['closed'] = True
            packet_row['active'] = False

        if i[12] == 1:
            packet_row['temporary_closed'] = True
        else:
            packet_row['temporary_closed'] = False

        packet_row['payment_methods'] = ["1", "2"]
        packet_row['legal'] = {"name": "Тамерлан", "form": "ООО"}

        packet.append(packet_row)

    with open(f'{brand}.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(packet, sort_keys=False, indent=4, ensure_ascii=False))


def get_empty_key(in_val: str, rep_val: str):
    if len(in_val) == 0:
        return rep_val
    else:
        return in_val


def update_hashed_addr():
    try:
        addr_hash = {}

        erp = MSSQLConnection('read_erp.json')
        with open('read_erp.sql', 'r') as q:
            q_erp = erp.select(q.read(), ())

        for erp_rec in q_erp:
            addr_hash[erp_rec[1]] = {"code": int(erp_rec[0]),
                                     'address_erp': {'region': erp_rec[3], 'city': erp_rec[4], 'postalcode': erp_rec[2],
                                                     'street': erp_rec[5], 'housenumber': erp_rec[6]}}
        cmdb = MSSQLConnection('read_cmdb.json')
        with open('read_cmdb.sql', 'r') as q:
            q_cmdb = cmdb.select(q.read(), ())

        for cmdb_rec in q_cmdb:
            key = cmdb_rec[1]
            if addr_hash.get(key) is not None:
                addr_hash[key]['lat'] = float(cmdb_rec[2])
                addr_hash[key]['lon'] = float(cmdb_rec[3])
                addr_hash[key]['active'] = cmdb_rec[4]

        ba_c = MSSQLConnection('read_buh.json')
        with open('read_buh.sql', 'r') as q:
            rb = ba_c.select(q.read(), ())

        for x in rb:
            for el in addr_hash.keys():
                if addr_hash[el]["code"] == x[0]:
                    addr = {}
                    if len(x[1]) > 0:
                        buf = dict(json.loads(x[1]))

                        addr["region"] = str(buf.get('area') + ' ' + buf.get('areaType')).lstrip().rstrip()
                        addr["city"] = get_empty_key(buf.get('city', ''),
                                                     str(buf.get('locality', '') + ' ' + buf.get('localityType',
                                                                                                 '').rstrip().lstrip()))
                        postal_code = get_empty_key(buf.get('ZIPcode', ''), '')
                        if len(postal_code) > 0:
                            addr["postalcode"] = postal_code
                        street = get_empty_key(buf.get('street', ''), '')
                        if len(street) > 0:
                            addr["street"] = str(street + ' ' + buf.get('streetType')).lstrip().rstrip()
                        else:
                            street = str(buf.get('territory') + ' ' + buf.get('territoryType')).rstrip().lstrip()
                            if len(street) > 0:
                                addr["street"] = street
                        addr["housenumber"] = get_empty_key(buf.get('houseNumber', ''), '0')
                        addr_hash[el]['address_buh'] = addr

        for k in addr_hash.keys():
            addr = {}
            addr["country"] = "RU"
            if addr_hash[k].get('lat'):
                addr["lat"] = addr_hash[k].get('lat')
                addr_hash[k].pop('lat')
            if addr_hash[k].get('lon'):
                addr["lon"] = addr_hash[k].get('lon')
                addr_hash[k].pop('lon')

            p1 = addr_hash[k].get('address_erp')
            p2 = addr_hash[k].get('address_buh')

            if p2 is None:
                addr['region'] = p1['region']
                addr['city'] = p1['city']
                addr['postalcode'] = p1['postalcode']
                addr['street'] = p1['street']
                addr['housenumber'] = p1['housenumber']
            else:
                addr['region'] = p2['region']
                addr['city'] = p2['city']
                addr['postalcode'] = p2.get('postalcode', p1.get('postalcode'))
                addr['street'] = p2.get('street', p1.get('street'))
                if p2.get('housenumber') != p1.get('housenumber'):
                    addr['housenumber'] = p1.get('housenumber')
                else:
                    addr['housenumber'] = p2.get('housenumber', p1.get('housenumber'))

            addr_hash[k]['address_buh'] = None
            addr_hash[k]['address_erp'] = None

            addr_hash[k].pop('address_buh')
            addr_hash[k].pop('address_erp')

            addr['housenumber'] = str(addr['housenumber']).replace(' ', '').replace(',', '')

            addr_hash[k]['address'] = addr

        with open(f'addr_hash.json', 'w', encoding='utf-8') as f:
            f.write(json.dumps(addr_hash, sort_keys=False, indent=4, ensure_ascii=False))
    except Exception as E:
        print(f'Hash build error: {E}')


update_hashed_addr()
write_packet('Покупочка')
write_packet('ПокупАЛКО')
