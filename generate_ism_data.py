#!/usr/bin/env python3
"""
SQL Insert Statement Generator for ISM Search POC
Generates sample data for location_master, skuloc, and rsvehr tables
"""

import random
import datetime
from typing import List, Dict, Any

class ISMDataGenerator:
    def __init__(self):
        # Configuration
        self.num_locations = 50  # Number of locations to generate
        self.num_skus = 1000     # Number of unique SKUs
        self.skuloc_density = 0.7  # Percentage of SKU-location combinations to create
        self.num_reserves = 500   # Number of reserve records
        
        # Sample data pools
        self.store_names = ['Times Square', 'Michigan Ave', 'Beverly Hills', 'Miami Beach', 
                           'Union Square', 'Fashion Valley', 'Galleria', 'Town Center',
                           'Fashion Island', 'Garden State', 'King of Prussia', 'Aventura']
        
        self.cities = [
            ('New York', 'NY'), ('Chicago', 'IL'), ('Los Angeles', 'CA'), ('Miami', 'FL'),
            ('San Francisco', 'CA'), ('Boston', 'MA'), ('Seattle', 'WA'), ('Dallas', 'TX'),
            ('Atlanta', 'GA'), ('Phoenix', 'AZ'), ('Denver', 'CO'), ('Portland', 'OR')
        ]
        
        self.regions = ['Northeast', 'Midwest', 'West Coast', 'Southeast', 'Southwest', 'Northwest']
        self.territories = ['EAST', 'CENTRAL', 'WEST', 'SOUTH']
        self.channels = ['D', 'R']  # D=Dotcom, R=Retail
        self.reservation_types = ['HR', 'SR', 'PR', 'MR']  # Hard, Soft, Pick, Merch
        self.reservation_status = ['A', 'E', 'C']  # Active, Expired, Cancelled
        self.reservation_programs = ['ECOMM', 'BOPIS', 'SHIP', 'STORE', 'EVENT']
        
        # Track generated IDs
        self.location_numbers = []
        self.sku_ids = []

    def generate_location_master_inserts(self) -> List[str]:
        """Generate INSERT statements for location_master table"""
        inserts = []
        
        # Generate stores (1001-1050)
        for i in range(1, self.num_locations + 1):
            loc_number = 1000 + i
            self.location_numbers.append(loc_number)
            
            # Determine location type
            if i <= 40:
                loc_type = 'STORE'
            elif i <= 45:
                loc_type = 'DC'
            else:
                loc_type = 'VENDOR'
            
            # Random city/state
            city, state = random.choice(self.cities)
            
            # Generate dates
            open_date = datetime.date(2015 + random.randint(0, 7), 
                                     random.randint(1, 12), 
                                     random.randint(1, 28))
            pi_date = open_date + datetime.timedelta(days=random.randint(30, 365))
            
            sql = f"""INSERT INTO location_master (
    loc_number, loc_name, loc_type, short_desc, loc_territory, loc_currency,
    loc_country, channel_name, region_number, region_name, district_number,
    district_name, last_pi_date, frame_store_id, frame_dc_id, whse_id,
    comingled_dc_flag, primary_serv_dc, alternate_serv_dc, city, state,
    zipCode, country, telephone, updated_at, loc_open_date
) VALUES (
    {loc_number},
    '{random.choice(self.store_names)} {city}',
    '{loc_type}',
    '{loc_type} in {city}',
    '{random.choice(self.territories)}',
    'USD',
    'US',
    'RETAIL',
    '{random.randint(1, 5)}',
    '{random.choice(self.regions)}',
    '{random.randint(100, 500)}',
    'District {random.randint(1, 20)}',
    '{pi_date}',
    'ST{loc_number}',
    'DC{2000 + random.randint(1, 5)}',
    'WH{2000 + random.randint(1, 5)}',
    'N',
    '{2000 + random.randint(1, 5)}',
    '{2000 + random.randint(1, 5)}',
    '{city}',
    '{state}',
    '{random.randint(10000, 99999)}',
    'USA',
    '{random.randint(200, 999)}-555-{random.randint(1000, 9999)}',
    NOW(),
    '{open_date}'
);"""
            inserts.append(sql)
        
        return inserts

    def generate_skuloc_inserts(self) -> List[str]:
        """Generate INSERT statements for skuloc table"""
        inserts = []
        
        # Generate SKU IDs (10000-10999)
        self.sku_ids = [10000 + i for i in range(self.num_skus)]
        
        # Generate SKU-Location combinations
        for sku_id in self.sku_ids:
            # Each SKU is in a random subset of locations
            num_locations = int(len(self.location_numbers) * random.uniform(0.1, self.skuloc_density))
            selected_locations = random.sample(self.location_numbers, num_locations)
            
            for loc_number in selected_locations:
                # Generate quantities
                available_qty = random.randint(0, 500)
                financial_onhand = available_qty + random.randint(0, 50)
                
                # Generate reserves (some items have no reserves)
                if random.random() < 0.3:  # 30% have reserves
                    ecomm_pick = random.randint(0, min(20, available_qty))
                    ecomm_pack = random.randint(0, min(10, available_qty - ecomm_pick))
                    merch_reserve = random.randint(0, min(15, available_qty - ecomm_pick - ecomm_pack))
                    dotcom_reserve = random.randint(0, min(25, available_qty))
                    retail_reserve = random.randint(0, min(30, available_qty))
                else:
                    ecomm_pick = ecomm_pack = merch_reserve = dotcom_reserve = retail_reserve = 0
                
                # Random channel
                channel = random.choice(self.channels) if random.random() < 0.8 else None
                
                sql = f"""INSERT INTO skuloc (
    sku_id, location_number, snb_qty, ecomm_pick_reserve, ecomm_pack_reserve,
    available_qty, merch_reserve_qty, lost_found_qty, pick_reserve_qty,
    is_comingle, financial_onhand_qty, oob_qty, total_in_transit,
    dotcom_reserve, retail_reserve, updated_ts
) VALUES (
    {sku_id},
    {loc_number},
    {random.randint(0, 10)},
    {ecomm_pick},
    {ecomm_pack},
    {available_qty},
    {merch_reserve},
    {random.randint(0, 5)},
    {random.randint(0, 20)},
    {1 if random.random() < 0.2 else 0},
    {financial_onhand},
    {random.randint(0, 10)},
    {random.randint(0, 50)},
    {dotcom_reserve},
    {retail_reserve},
    NOW()
);"""
                inserts.append(sql)
        
        return inserts

    def generate_rsvehr_inserts(self) -> List[str]:
        """Generate INSERT statements for rsvehr table"""
        inserts = []
        
        for i in range(1, self.num_reserves + 1):
            # Random SKU and location from existing ones
            sku_id = random.choice(self.sku_ids)
            loc_number = random.choice(self.location_numbers)
            
            # Generate timestamps
            start_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30))
            end_date = start_date + datetime.timedelta(days=random.randint(1, 14))
            
            # Generate reservation data
            channel = random.choice(self.channels)
            ats_flag = 'Y' if random.random() < 0.7 else 'N'
            res_type = random.choice(self.reservation_types)
            res_status = random.choice(self.reservation_status)
            res_program = random.choice(self.reservation_programs)
            res_qty = random.randint(1, 50)
            
            sql = f"""INSERT INTO rsvehr (
    division_number, location_number, sku_id, channel, ats_flag,
    reservation_type, hard_reservation_start_ts, hard_reservation_end_ts,
    hard_reservation_qty, hard_reservation_start_time_upd,
    hard_reservation_end_time_upd, reservation_status, reservation_program,
    reservation_user_id, reservation_last_modified_ts
) VALUES (
    {random.randint(1, 5)},
    {loc_number},
    {sku_id},
    '{channel}',
    '{ats_flag}',
    '{res_type}',
    '{start_date.strftime("%Y-%m-%d %H:%M:%S")}',
    '{end_date.strftime("%Y-%m-%d %H:%M:%S")}',
    {res_qty},
    '{start_date.strftime("%Y-%m-%d %H:%M:%S")}',
    '{end_date.strftime("%Y-%m-%d %H:%M:%S")}',
    '{res_status}',
    '{res_program}',
    'USER{random.randint(100, 999)}',
    NOW()
);"""
            inserts.append(sql)
        
        return inserts

    def generate_all(self, output_file: str = 'ism_sample_data.sql'):
        """Generate all SQL inserts and write to file"""
        print(f"Generating sample data...")
        print(f"- Locations: {self.num_locations}")
        print(f"- SKUs: {self.num_skus}")
        print(f"- SKU-Location density: {self.skuloc_density * 100}%")
        print(f"- Reserve records: {self.num_reserves}")
        
        with open(output_file, 'w') as f:
            # Header
            f.write("-- ISM Search POC Sample Data\n")
            f.write(f"-- Generated on: {datetime.datetime.now()}\n")
            f.write("-- Configuration:\n")
            f.write(f"-- - Locations: {self.num_locations}\n")
            f.write(f"-- - SKUs: {self.num_skus}\n")
            f.write(f"-- - Reserve records: {self.num_reserves}\n\n")
            
            # Optional cleanup
            f.write("-- Optional: Clean existing data\n")
            f.write("-- TRUNCATE TABLE rsvehr;\n")
            f.write("-- TRUNCATE TABLE skuloc;\n")
            f.write("-- TRUNCATE TABLE location_master;\n\n")
            
            # Location master inserts
            f.write("-- ========================================\n")
            f.write("-- Location Master Data\n")
            f.write("-- ========================================\n\n")
            location_inserts = self.generate_location_master_inserts()
            for sql in location_inserts:
                f.write(sql + "\n\n")
            
            # SKULOC inserts
            f.write("\n-- ========================================\n")
            f.write("-- SKULOC Data\n")
            f.write("-- ========================================\n\n")
            skuloc_inserts = self.generate_skuloc_inserts()
            for sql in skuloc_inserts:
                f.write(sql + "\n\n")
            
            # RSVEHR inserts
            f.write("\n-- ========================================\n")
            f.write("-- RSVEHR Data\n")
            f.write("-- ========================================\n\n")
            rsvehr_inserts = self.generate_rsvehr_inserts()
            for sql in rsvehr_inserts:
                f.write(sql + "\n\n")
            
            # Footer
            f.write("\n-- End of sample data\n")
            f.write("COMMIT;\n")
        
        print(f"\nSQL file generated: {output_file}")
        print(f"Total INSERT statements: {len(location_inserts) + len(skuloc_inserts) + len(rsvehr_inserts)}")
        print(f"\nEstimated records:")
        print(f"- location_master: {len(location_inserts)}")
        print(f"- skuloc: {len(skuloc_inserts)}")
        print(f"- rsvehr: {len(rsvehr_inserts)}")

if __name__ == "__main__":
    # Create generator with custom settings
    generator = ISMDataGenerator()
    
#     Customize the numbers if needed
    generator.num_locations = 100
    generator.num_skus = 5000
    generator.skuloc_density = 0.5
    generator.num_reserves = 1000
    
    # Generate the SQL file
    generator.generate_all('ism_sample_data.sql')
    
    print("\nTo use the generated data:")
    print("1. Review the generated SQL file")
    print("2. Run: mysql -u your_user -p your_database < ism_sample_data.sql")
    print("3. Or copy/paste the SQL into your database client")
