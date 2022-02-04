import mysql.connector
import json
import re
import pandas as pd
pd.options.display.max_columns = None

class DB:
    def __init__(self) -> None: 
        with open('config/db-creds.json','r') as readfile:
            db_creds = json.load(readfile)

        # Connecting to the server
        self.conn = mysql.connector.connect(**db_creds)
        self.cur = self.conn.cursor()


    def __del__(self):
        try:
        # Disconnecting from the server
            self.conn.close()
        except Exception as e:
            # print(e)
            pass

    def create_tables(self):
        # self.conn._execute_query("DROP TABLE json_to_sql_table;")
        # self.conn.commit()
        self.cur.execute("CREATE TABLE json_to_sql_table (name varchar(100),phone varchar(100),email varchar(200),address varchar(255),region varchar(100),country varchar(100),list integer,postalzip varchar(200),currency varchar(100));")
        self.conn.commit()

    def load_data(self):
        with open('database/sample_data_for_assignment.json','r') as rf:
            data = json.load(rf)

        for row in data["data"]:
            # Converting all pin codes to strings
            row[7] = str(row[7])
            try:
                self.cur.execute("INSERT INTO json_to_sql_table VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",tuple(row))
                # self.conn.commit()
            except Exception as e:
                print(e)
                break

    def load_to_pandas(self):
        self.cur.execute("SELECT * FROM json_to_sql_table;")
        rows = self.cur.fetchall()
        self.df = pd.DataFrame(rows,columns=["name",
		"phone",
		"email",
		"address",
		"region",
		"country",
		"list",
		"postalZip",
		"currency"])
        # print(df)

    def change_email(self):
        '''Changes email pattern to email@gmail.com'''
        for i in range(len(self.df)):
            email = self.df.loc[i,"email"]
            new_email = email.split('@')[0] + "@gmail.com"
            self.df.loc[i,"email"] = new_email
        # print(self.df)

    def change_postal_code(self):
        for i in range(len(self.df)):
            address = self.df.loc[i,"postalZip"]
            try:
                new_address = int(re.sub("[^0-9]+","",address))
            except Exception as e:
                print(address)
            self.df.loc[i,"postalZip"] = new_address

        # print(self.df)
        # print(self.df.dtypes)

    def convert_phone_numbers(self,phone_no) -> str:
        phone_no = re.sub("[^0-9]+","",phone_no)
        return_string = ''
        for i in range(0,len(phone_no),2):
            try:
                temp_no = int(phone_no[i]+phone_no[i+1])
            except IndexError:
                return return_string
            if temp_no < 65:
                return_string += "O"
            else:
                return_string += chr(temp_no)

        return return_string

    def modify_Dataframe_for_phone_no(self):
        for i in range(len(self.df)):
            self.df.loc[i,"phone"] = self.convert_phone_numbers(self.df.loc[i,"phone"])

        print(self.df)
            

        

if __name__ == '__main__':
    D = DB()
    # D.create_tables()
    # D.load_data()
    D.load_to_pandas()
    D.change_email()
    D.change_postal_code()
    D.modify_Dataframe_for_phone_no()


'''
Final pandas output after all modifications.
                 name  phone                              email  \
0     Winifred Branch  OOOOc                    in.mi@gmail.com   
1    Brielle Davidson  OOOOO          venenatis.lacus@gmail.com   
2     Summer Mcfadden  OO^^B                  orci.in@gmail.com   
3       Geoffrey Long  OOKOb  placerat.eget.venenatis@gmail.com   
4        Barry Conner  OOCOH                    lorem@gmail.com   
..                ...    ...                                ...   
495  Adrian Rodriquez  OO_RO    non.quam.pellentesque@gmail.com   
496        Sonia Lamb  OOLIO             suscipit.est@gmail.com   
497       Kylee Silva  O`OOO             nibh.lacinia@gmail.com   
498     Jeanette Todd  OTOO_                     nunc@gmail.com   
499       Gil Hawkins  OOOOO                   luctus@gmail.com   

                           address          region             country  list  \
0           Ap #530-5652 Arcu. Rd.  Rio de Janeiro             Nigeria    19   
1     Ap #630-7765 Molestie Avenue           Aisén           Australia    13   
2    P.O. Box 410, 6638 Sed Avenue          Biobío             Belgium     3   
3          783-9358 Aliquet Street  North Sulawesi       United States     1   
4                   795 Mauris Rd.          Vaupés               China     7   
..                             ...             ...                 ...   ...   
495       107-4830 Lobortis Avenue   Noord Brabant  Russian Federation     1   
496                8062 Lectus St.   Kurgan Oblast              Sweden     9   
497       Ap #336-1000 Diam Street         Drenthe             Vietnam     5   
498      Ap #680-5749 Neque Avenue      Chandigarh             Ireland     9   
499                 844-2805 A St.         Atacama            Colombia     3   

    postalZip currency  
0    66242403   $14.59  
1       18317   $56.16  
2        8581   $36.45  
3       28826   $69.58  
4      335049   $77.68  
..        ...      ...  
495     49343   $90.17  
496    393741   $56.21  
497     70801   $63.16  
498      3765   $43.01  
499    651193   $22.87  

[500 rows x 9 columns]

'''