# Install all packages (rarely needed more than once)
install_packages <- function() {
  install.packages("foreach")
  install.packages("doParallel")
  install.packages("ggplot2")
  install.packages("futile.logger")
  install.packages("lubridate")
  install.packages("dplyr")
  install.packages("reshape")
}

# Prepare
prepare_environment <- function(cores=14) {
  require(gridExtra)
  library(digest)
  library(foreach)
  library(doParallel)
  library(ggplot2)
  library(futile.logger)
  library(lubridate) # work with dates
  library(dplyr)     # data manipulation (filter, summarize, mutate)
  library(colorspace)
  library(reshape2)
  flog.threshold(INFO)
  registerDoParallel(cores)
}

sample_plot_data <- function(name="Unknown", width=960, height=480, input_data=data.frame()) {
  # Generate output plot - save to PNG
  
  filename = paste(name,"_annual.png",sep = "")
  
  clrs <- rainbow_hcl(ncol(input_data)-1, c = 50, l = 70, start = 0)
  
  #input_data <- subset(input_data, date > as.POSIXct('2019-01-01 00:00:00'))
  
  flog.info("Creating plot filename: %s", filename)
  
  p <- ggplot(data=input_data,aes(x=date,y=measurement,colour=county)) + geom_point( alpha = 0.3, size=0.2)
  p <- p + guides(colour = guide_legend(override.aes = list(title="Cities", title.position="bottom",shape = "o", alpha=1,size=5)))
  p <- p + labs(title = paste("Graph of",name,"ouput over time"),
                x = "Date",
                y = "Gas measurement")
  
  ggsave(filename,width = width/5, height = height/5, units = "mm", dpi=300)
}

reform_airdata <- function(locations=list(), filenames=list(), cumulative_results=NA) {
  
  
  intermediate_result = foreach (filename=filenames, .combine=rbind) %dopar% {
    rawdata = read.csv(filename)
    
    matching_state_city = FALSE
    for (row in 1:nrow(locations)) {
      matching_state_city = matching_state_city | (rawdata$State.Name==locations[row,"State"] & rawdata$County.Name==locations[row,"City"] & !is.na(rawdata$Sample.Measurement) )
    }
    
    subpart = rawdata[ which(matching_state_city),]
    date_string_vec = paste(subpart$Date.Local,subpart$Time.Local)
    datetime_vec <- as.POSIXlt(date_string_vec)
    data_cache = data.frame("date" = datetime_vec, county=subpart$County.Name,  measurement=subpart$Sample.Measurement)
    data_cache
  }
  
  flog.info(" - extracted %s rows, %s columns from %s files [last date: %s] (intermediate: %s)", nrow(intermediate_result), ncol(intermediate_result), length(filenames), intermediate_result[nrow(intermediate_result)-1,1], colnames(intermediate_result))
  
  cleaned_result <- intermediate_result %>%
    group_by(date,county) %>%
    summarise(measurement = mean(measurement))
  
  flog.info(" - averaged data %s rows, %s columns (cleansed: %s)", nrow(cleaned_result), ncol(cleaned_result), colnames(cleaned_result))
  return(cleaned_result)
}

get_airdata_cache <- function(gas=NA,locations=NA,data_dir=NA) {
  # Load cached file (if exists), else generate
  hash <- digest(locations,algo="md5", serialize=F)
  cached_data_filename = paste(gas,"_",hash,".rds",sep = "")
  if (file.exists(cached_data_filename)) {
    flog.info("Found cache for %s gas",gas)
    start_time <- Sys.time()
    gas_compare <- readRDS(cached_data_filename)
    end_time <- Sys.time()
    flog.info("Cache load for %s gas took %0.2f seconds (ncol: %s, nrows: %s)",gas,end_time-start_time,ncol(gas_compare),nrow(gas_compare))
  }
  else {
    gas_compare <- NA
    input_filenames = get_hourly_airdata(gas=gas,dir=data_dir)
    
    # For each location - cache it
    start_time <- Sys.time()
    
    gas_compare <- reform_airdata(locations=locations, filenames=input_filenames, cumulative_results=gas_compare)
    end_time <- Sys.time()
    saveRDS(gas_compare, file = cached_data_filename)
    flog.info("Cache generation for %s gas took %0.2f seconds (ncol: %s, nrows: %s)",gas,end_time-start_time,ncol(gas_compare),nrow(gas_compare))
  }
  
  return(gas_compare)
}

get_hourly_airdata <- function(gas=0000,dir=NA) {
  
  if (!is.na(dir)) {
    setwd(dir)
  }
  
  pattern = paste(".*_",gas,"_.*.csv",sep = "")
  filenames <- list.files(getwd(), pattern=pattern, full.names=TRUE)
}


##############################################
##############################################

main <- function() {
  #install_packages()
  prepare_environment(cores=14)
  
  
  locations = noquote(matrix(c("California","Los Angeles",1002,"New York","Bronx",0003),ncol=3,byrow=TRUE))
  colnames(locations) <- c("State","City","Site")
  
  gasses = noquote(matrix(c("Ozone",44201,"Sulfur dioxide",42401,"Carbon monoxide",42101,"Nitrogen dioxide",42602),ncol=2,byrow=TRUE))
  colnames(gasses) <- c("Name","Code")
  
  
  
  # Iterate over gasses and generate one vector
  for (row in 1:nrow(gasses)) {
    # Get the data from the requested files
    gas_compare <- get_airdata_cache(gas=gasses[row,"Code"],locations=locations,data_dir="/home/bijan/EPA")
    
    # Generate output plot - save to PNG
    start_time <- Sys.time()
    sample_plot_data(name=gasses[row,"Name"],input_data=gas_compare)
    end_time <- Sys.time()
    flog.info("Took %0.2f seconds to graph %s", end_time-start_time, gasses[row,"Name"])
  }
}

