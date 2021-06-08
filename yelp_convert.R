library(jsonlite)
library(sets)
#Read in the JSON file.
lines <- readLines(file("yelp.json"))
fromJSON(lines[1])

#For each line in the file, convert to JSON format.  
#This will take a little bit of time:
all_rows<-lapply(1:length(lines),
                 function(x){
                   if(lines[x]!=""){
                     fromJSON(lines[x])
                   }else{}
                   
                 })

#Inspect the possible names:
names(all_rows[[1]])

#Let's inspect data types:
str(all_rows[[1]])

#We know we want business id, name, state, stars, review count.
#These are all easy to grab.  However, the attributes and categories
#are themselves lists.  So how can we grab these?

#We can structure this data by simply creating a new row for each
#attribute.  THe unit of analysis would thus change from being
#business to business-attribute.  Before we do this, however, let's get an
#entire list of attribute names.  Keeping track of unique elements is easy with
#the package "sets":
install.packages("sets")
library(sets)

#Create an empty set of attributes:
attr<-gset()

#Now iterate through all the "rows" (i.e. businesses): (This will take some time)
for(row in all_rows){
  #Grab the attribute names:
  attr_names<-gset(names(row$attributes))
  #Add to the set, which will only keep the unique elements since we are (1) operating
  #on sets rather than vectors or lists and (2) set union operations ONLY keep unique
  #elements:
  attr<-set_union(attr,attr_names)
}

#Now that we know which attributes are present in the data, 
#it is not time to determine the type of data each attribute stores.  We can do this
#by exploring each attribute, determining which row has that attribute, and looking at the value:
value_sample<-list()
for(a in attr){
  row<-all_rows[[1]]
  i<-2
  while(!is.element(a,names(row$attributes))){
    i<-i+1
    row<-all_rows[[i]]
  }
  value_sample[[a]]<-all_rows[[i]]$attributes[[a]]
}

#For now, we just want the attributes that have True/False values.
#McNeil can settle with this for now.  So, let's extract all attribute
#names that only have True/False values:
attr_names<-c()
for(attribute_name in names(value_sample)){
  value<-value_sample[[attribute_name]]
  if(value=="True" || value=="False"){
    attr_names<-c(attr_names,attribute_name)
  }
}

#Awesome!  Now, let's look at categories.  In a similar procedure, let's first
#explore what this looks like:
all_rows[[1]]$categories

#We can see it is a single string.  We must split this.  We are only looking for
#the primary category, which we will assume is the first one listed on the list.
#So, we will need to first split this string by commas, then extract the first 
#entry.  We can do this in the primary loop that iterates through the "rows" in
#our current list.  

#So, let's begin the construction.  We now know that we will have the following columns,
#where one row will represent a business-attribute combination:
##business id
##name 
##state 
##stars 
##review_count
##attribute_name
##primary_category

#How can we construct this?  First, create a dummy row just to get the dataframe created:
the_data<-data.frame(business_id = c(0),
                     business_name = c("Test"),
                     state=c("MA"),
                     stars=c(0),
                     review_count=c(0),
                     primary_category=c("Primary"),
                     attribute_name=c("Test")
                     )
the_data

#Later, we will delete the first row.  This now makes it easy to append.
#All we need to do is construct a row, and "bind" it to the end of the
#data frame using "rbind".  This works only if the vector you are binding 
#to the dataframe has the same number of elements.  Let do it (This will take
#some time!)

library(parallel)

cl<-makeCluster(detectCores())
clusterExport(cl,"all_rows")
clusterExport(cl,"attr_names")

#for(row in all_rows){
results<-parLapply(cl,1:length(all_rows),function(i){
#  write.csv(c(1),paste(i,".csv",sep=""))
  row<-all_rows[[i]]
  row
  #We need to first create the values for this part of each row.
  #Recall that each row represents a business-attribute.  First,
  #we need to construct the information related to the business.  Then
  #we will construct the information for each attribute of that business
  #and append the row for each attribute.  First,  business information:
  bid<-row$business_id
  bname<-row$name
  bstate<-row$state
  bstars<-row$stars
  breviews<-row$review_count
  bpcat<-"None"
  
  #Now process the primary category
  #If this row has "categories" specified:
  if(is.element("categories",names(row))){
    primary_cat<-row$categories[1]
    #And if there is a value for categories other than just empty or NULL:
    if(!is.null(primary_cat) && primary_cat!=""){
      #Split this by comma, grab the first element of the list, 
      #and then the first element of the returned vector of values:
      bpcat<-strsplit(primary_cat,",")[[1]][1]
    }
  }
  
  the_data<-data.frame(business_id = c(0),
                       business_name = c("Test"),
                       state=c("MA"),
                       stars=c(0),
                       review_count=c(0),
                       primary_category=c("Primary"),
                       attribute_name=c("Test")
  )
  #Now we process each attribute, construct a new row, and append it to the dataframe:
  if(is.element("attributes",names(row))){
    cattr<-names(row$attributes)
    for(battr in cattr){
      if(is.element(battr,attr_names)){
        if(row$attributes[[battr]]=="True"){
          new_row<-c(bid,bname,bstate,bstars,breviews,bpcat,battr)
          #the_data<-rbind(the_data,new_row)
          the_data<-rbind(the_data,new_row)
        }
      }
    }
  }
  the_data
})
library(data.table)

the_data<-rbindlist(results)
the_data<-the_data[the_data$business_id!="0"]
#Now we have our data, print to csv:
write.csv(the_data,"mcneil_data.csv")
