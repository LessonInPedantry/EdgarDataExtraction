library(data.table)

#Data can be pulled down from https://www.sec.gov/dera/data/financial-statement-data-sets.  Each zip file 
#to be analyzed should be saved to a folder called \\data.  Cleaned data will be saved to \\cleaned_data.

#Script Assumes that there are two directories as created below.  IF zip files aren't in the
#data directory created below, the script will fail.  The directories are created for user convenience.
directory <- "C:\\Users\\jGoodbread\\Desktop\\Module 8\\"
dir.create(file.path(directory, "data"), showWarnings = FALSE)
dir.create(file.path(directory, "cleaned_Data"), showWarnings = FALSE)
zip_files <- list.files(paste0(directory,"data\\"), pattern = "*.zip", ignore.case = TRUE)

#Extract analysis quarters
Quarters <- unlist(strsplit(zip_files,"\\."))
Quarters <- Quarters[Quarters != "zip"]

#Extract Zip Files.  Only Run to update data
# extract_data <- function(Quarter){
#   print(paste0("Extracting data from ", Quarter, ".zip"))
#   dir.create(file.path(directory, "data\\",Quarter), showWarnings = FALSE)
#   unzip(paste0(directory,"data\\",Quarter,".zip"), exdir = paste0(directory,paste0("data\\",Quarter)))
# }
#sapply(Quarters,extract_data)

#Function to ingest each quarter's data.  There are four datasets per quarter stored in tab delimited text files
ingest_data <- function(Quarter){
  print(paste0("Ingesting data from Quarter: ", Quarter))
  num_table <- fread(file = paste0(directory,"\\data\\",Quarter,"\\num.txt"), sep = "\t", quote = "")
  pre_table <- fread(file = paste0(directory,"\\data\\",Quarter,"\\pre.txt"), sep = "\t", quote = "")
  sub_table <- fread(file = paste0(directory,"\\data\\",Quarter,"\\sub.txt"), sep = "\t", quote = "")
  tag_table <- fread(file = paste0(directory,"\\data\\",Quarter,"\\tag.txt"), sep = "\t", quote = "")

  list(num_table,pre_table,sub_table,tag_table)
}

data_list <- lapply(Quarters,ingest_data)

#Vertical merge all quarters data in to four tables, one for each of 4 quarterly data file
print("Vertical Merging Datasets")
num_table <- data.table()
pre_table <- data.table()
sub_table <- data.table()
tag_table <- data.table()

for(data_group in seq(1:length(data_list))){
    num_table <- rbind(num_table,data_list[[data_group]][[1]])
    pre_table <- rbind(pre_table,data_list[[data_group]][[2]])
    sub_table <- rbind(sub_table,data_list[[data_group]][[3]])
    tag_table <- rbind(tag_table,data_list[[data_group]][[4]])
}

#save(num_table,pre_table,sub_table,tag_table, file = paste0(directory,"\\cleaned_data\\Raw SEC Data All Companies.RData"))
#load(file = paste0(directory,"\\cleaned_data\\Raw SEC Data All Companies.RData"))
#Cleaning Data
print("Cleaning Data")

#Remove submissions that are not Columbia Sportswear
competitor_set <- c("CABELAS INC","NIKE INC","HANESBRANDS INC.","SKECHERS USA INC","CANADA GOOSE HOLDINGS INC.",
                    "ACADEMY SPORTS & OUTDOORS, INC.","AMERICAN OUTDOOR BRANDS CORP","AMERICAN OUTDOOR BRANDS, INC.",
                    "JOHNSON OUTDOORS INC","VISTA OUTDOOR INC.")

sub_table <- sub_table[name == "COLUMBIA SPORTSWEAR CO" | name %in% competitor_set]

pre_table <- pre_table[adsh %in% sub_table$adsh,]
num_table <- num_table[adsh %in% sub_table$adsh,]

num_table <- num_table[coreg == ""]

other_Currencies <- c("JPY","CNY", "CAD")
num_table <- num_table[!uom %in% other_Currencies]
num_table[,UniqueID := paste0(as.character(adsh),"_",as.character(ddate),"_",as.character(qtrs))]


#Cleaning num_table - num_table contains values in reports submitted to the SEC.  Its built as row per value.
#The goal is to flip the data wide, identify duplicated reported values inside each quarter and compress
#All values into a table by quarter.  This will allow for reporting by quarter across Q1 2017 - Q2 2023.

print("Transforming Data Values Wide (num_table)")
num_table_wide <- dcast(num_table, UniqueID+adsh + ddate +qtrs ~ tag, value.var = "value")
num_table_wide[, Date_Coded := as.Date(as.character(ddate),format = "%Y%m%d")]

num_table_wide <- num_table_wide[year(Date_Coded) > 2016]

num_table_wide <- num_table_wide[month(Date_Coded) %in% 1:3, Quarter_Year := paste0("Q1_",as.character(year(Date_Coded)))]
num_table_wide <- num_table_wide[month(Date_Coded) %in% 4:6, Quarter_Year := paste0("Q2_",as.character(year(Date_Coded)))]
num_table_wide <- num_table_wide[month(Date_Coded) %in% 7:9, Quarter_Year := paste0("Q3_",as.character(year(Date_Coded)))]
num_table_wide <- num_table_wide[month(Date_Coded) %in% 10:12, Quarter_Year := paste0("Q4_",as.character(year(Date_Coded)))]

num_table_wide$UniqueID <- NULL
company_append <- sub_table[,data.table(adsh,name)]
num_table_wide <- plyr::join(x = num_table_wide, y = company_append, by = "adsh", type = "left")

num_table_wide <- num_table_wide[, Company_Quarter := paste0(Quarter_Year,"_",name)]

Quarters_From_Date <- unique(num_table_wide$Company_Quarter)
Condenced_num_table <- num_table_wide[is.na(Quarter_Year)]

#Here we condense the wide table of values into a single row per quarter.  The same value occurs many times
#across a single quarter's submissions.  This is also a data quality check as there will be warnings if 
#submission data with different values is collapsed into a single quarter.  I am aware that I misspelled condensed
#in the variable naming conventions.  I could change it but users can interpret what I mean as the spelling
#isn't that far off and the script works as is.

for (quarter in Quarters_From_Date){
  print(paste0("Condencing Data for Quarter: ", quarter))
  Condenced_quarter_data <- num_table_wide[is.na(Quarter_Year)]
  Condenced_quarter_data <- Condenced_quarter_data[NA_integer_]
  Quarter_Data <- num_table_wide[Company_Quarter == quarter,]

  for(field in names(num_table_wide)){
    #print(field)
    unique_values <- unique(Quarter_Data[,get(field)])
    unique_values <- unique_values[!is.na(unique_values)]
    #print(unique_values)
    if(length(unique_values) == 1){
      #Condenced_quarter_data[1,get(field) := unique(Quarter_Data[,get(field)])]
      eval(parse(text= paste0("Condenced_quarter_data[1,",field," := unique_values]")))
    }
  }

  Condenced_num_table <- rbind(Condenced_num_table,Condenced_quarter_data)
}

#save(Condenced_num_table,num_table_wide,pre_table,sub_table,tag_table, file = paste0(directory,"\\cleaned_data\\Columbia Sportswear Condensed Data.rdata"))
#load(file = paste0(directory,"\\cleaned_data\\Columbia Sportswear Condensed Data.rdata"))

Condenced_num_table$adsh <- NULL
Condenced_num_table$ddate <- NULL
Condenced_num_table$qtrs <- NULL

rm(data_list)
rm(Condenced_quarter_data)

#Cleaning the tag_table.  There is one tag per type of value contained in all SEC filings that have been put into
#the SEC publicly facing data.  We are only using certian years and many tags are not of use to us.  This code
#filters down to the useful tags and the correct versions thereof.  It is unfortunate, but not all value fields
#in num.txt are associated with a tag in tag.txt.  We therefore extract as much as we can understanding that
#70 value fields have no field information in tag.txt per length(setdiff(useful_tags,unique(tag_table$tag)))
#there are 68 fields when using setdiff(unique(num_table$tag),unique(tag_table$tag)).  This makes sense
#as I added two fields to the condensed values, Quarter_Year and Year.

useful_tags <- names(Condenced_num_table)
#backup_tag <- tag_table
tag_table <- tag_table[tag %in% useful_tags,]
tag_table <- tag_table[order(tag,version)]
tag_table <- unique(tag_table)
 
useful_tag_table <- tag_table[NA_integer_]

for (useful_tag in unique(tag_table$tag)){
  temp_tag_table <- tag_table[tag == useful_tag,]
  useful_tag_table <- rbind(useful_tag_table,temp_tag_table[nrow(temp_tag_table),])
}

useful_tag_table <- useful_tag_table[!is.na(tag),]

rm(num_table)
rm(Quarter_Data)
rm(tag_table)
rm(temp_tag_table)

sub_table[, filed := as.Date(as.character(filed),format = "%Y%m%d")]
sub_table[, period := as.Date(as.character(period),format = "%Y%m%d")]
sub_table <- sub_table[year(period) >= 2017,]

sub_table[month(period) %in% 1:3, Quarter_Year := paste0("Q1_",as.character(year(period)))]
sub_table[month(period) %in% 4:6, Quarter_Year := paste0("Q2_",as.character(year(period)))]
sub_table[month(period) %in% 7:9, Quarter_Year := paste0("Q3_",as.character(year(period)))]
sub_table[month(period) %in% 10:12, Quarter_Year := paste0("Q4_",as.character(year(period)))]



#Save the cleaned SEC data
save(Condenced_num_table,num_table_wide,pre_table,sub_table,useful_tag_table, file = paste0(directory,"\\cleaned_data\\Cleaned_SEC_Data.RData"))

write.csv("Condenced_num_table", file = paste0(directory,"\\cleaned_data\\Cleaned_Numeric_Data.csv"))
write.csv("pre_table", file = paste0(directory,"\\cleaned_data\\pre_table.csv"))
write.csv("sub_table", file = paste0(directory,"\\cleaned_data\\sub_table.csv"))
write.csv("useful_tag_table", file = paste0(directory,"\\cleaned_data\\useful_tag_table.csv"))


#load(file = paste0(directory,"\\cleaned_data\\Cleaned_SEC_Data.RData"))


