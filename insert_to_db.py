'''
Created on 18 Nov 2016

@author: aled
'''
import sys
import getopt
import os
import pyodbc

class test_input():

    def __init__(self):
        self.folder_path = ""
        self.usage = "test.py -p <folder_path>"
        
         # variables for the database connection
        self.cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=GSTTV-MOKA; DATABASE=devdatabase;")
        self.cursor = self.cnxn.cursor()
        
        #self.dbconnectstring="DRIVER={SQL Server};SERVER=gsttv-MOKA;PORT=1433;DATABASE=Mokadata;UID=MokaRead;PWD=MokaRead"
        self.dictionary={}
        
        self.select_qry=""
        self.insert_query=""
        
        self.select_qry_exception = ""
        self.backup_qry=""
        
        self.gene=False
        

    def set_depth_of_coverage_path(self, argv):
        try:
            opts, args = getopt.getopt(argv, "p:", ["path="])
        except getopt.GetoptError:
            print self.usage
            sys.exit(2)

        for opt, arg in opts:
            if opt == 'h':
                print self.usage
                sys.exit()
            elif opt in ("-p", "--path"):
                self.folder_path = arg

    def read_depth_of_coverage_files(self):
        for file in os.listdir(self.folder_path):
            filename=file.split("_")
            DNAnumber=filename[2]
            gene_summary_file=open(self.folder_path+"/"+file,'r')
            self.dictionary[DNAnumber] = {}
            for line in gene_summary_file:
                if line.startswith("Gene") or line.startswith("UNKNOWN") or line.startswith("LOC100287896") or line.startswith("LOC81691")or line.startswith("TARP"):
                    pass
                else:
                    splitline=line.split('\t')
                    gene=splitline[0]
                    avg_coverage=splitline[2]
                    coverage20x=splitline[8]
                    self.dictionary[DNAnumber][gene]=((avg_coverage,coverage20x.rstrip()))
        #for dna in self.dictionary:
            #for gene in self.dictionary[dna]:
                #print dna, gene, self.dictionary[dna][gene]

    def insert_to_db(self):
        for dnanumber in self.dictionary:
            self.select_qry = "select InternalPatientID from dbo.DNA where DNANumber = '"+dnanumber+"'"
            print self.select_qry
            self.select_qry_exception = "can't get the internalpatientID"
            InternalPatientID=self.select_query()[0][0]

            for gene in self.dictionary[dnanumber]:
                self.gene=True
                self.select_qry = "select GenesHGNCID from dbo.GenesHGNC_current where ApprovedSymbol = '"+gene+"'"
                self.backup_qry = "select GenesHGNCID from dbo.GenesHGNC_current where PreviousSymbols LIKE '%"+gene+"%'"
                self.select_qry_exception = "can't get the GenesHGNCID for "+gene
                GenesHGNCID=self.select_query()[0][0]
                avg_coverage=self.dictionary[dnanumber][gene][0]
                above20x=self.dictionary[dnanumber][gene][1]
                self.insert_query="insert into dbo.Coverage (Gene,InternalPatientID,avg_coverage,above20X,DNAnumber) values ("+str(GenesHGNCID)+","+str(InternalPatientID)+","+str(avg_coverage)+","+str(above20x)+","+str(dnanumber)+")" 
                print self.insert_query
                self.insert_query_function()

    def select_query(self):
        '''This function is called to retrieve the whole result of a select query '''
        # Perform query and fetch all
        result = self.cursor.execute(self.select_qry).fetchall()
        
        # return result
        if result:
            return(result)
        elif self.gene:
            result = self.cursor.execute(self.backup_qry).fetchall()
            if result:
                return(result)
            else:
                raise Exception(self.select_qry_exception)
        else:
            raise Exception(self.select_qry_exception)

    def insert_query_function(self):
        '''This function executes an insert query'''
        # execute the insert query
        self.cursor.execute(self.insert_query)
        self.cursor.commit()
            


if __name__ == '__main__':
    a = test_input()
    a.set_depth_of_coverage_path(sys.argv[1:])
    a.read_depth_of_coverage_files()
    a.insert_to_db()

