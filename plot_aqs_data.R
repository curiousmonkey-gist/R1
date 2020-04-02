#' Install packages required for this script

install_packages <- function() {
  install.packages("foreach")
  install.packages("doParallel")
  install.packages("ggplot2")
  install.packages("futile.logger")
  install.packages("lubridate")
  install.packages("dplyr")
  install.packages("reshape")
}

#' Configure the environment - import libraries
#'
#' @param cores Number of cores to use when parallelizing workloads
#' @examples
#' prepare_environment(cores=64)
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

#' A sample plot function saving plots as PNGs
#'
#' @param name The prefix name to use for the file
#' @param width The width of the plot
#' @param height The height of the plot
#' @param input_data The tidy data with "date, location, value"
#' @examples
#' sample_plot_data("Carbon_monoxide", width=960, height=480, input_data=data)
sample_plot_data <- function(name="Unknown", width=960, height=480, input_data=data.frame()) {
  # Generate output plot - save to PNG

  
  filename = paste(sub(" ","_", name),"_annual.png",sep = "")
  
  theme_set(theme_minimal()) 
  
  #input_data <- subset(input_data, date > as.POSIXct('2018-01-01 00:00:00'))
  
  # Add a column representing the week
  input_data <- input_data %>%
    mutate(week = as.POSIXct(floor_date(as.Date(date) - 1, "weeks") + 1))
  
  # Create dataset with week start as input
  input_data_week <- input_data %>%
    group_by(week,county) %>%
    summarise(avg_over_wk = median(measurement))

  flog.info("Creating plot filename: %s", filename)

  p <- ggplot(data=input_data,aes(x=date,y=measurement,colour=county))
  p <- p + geom_point(alpha = 0.05, size=0.05, shape=".")
  p <- p + labs(title = paste("Graph of",name,"ouput over time"),
                x = "Date",
                y = "Gas measurement")
  p <- p + guides(colour = guide_legend(override.aes = list(title="Cities", title.position="bottom",shape = "o", alpha=1,size=5)))
  p <- p + geom_line(data=input_data_week,aes(x=week,y=avg_over_wk,colour=county),linetype = "solid", size=0.5,alpha=0.7)
  p <- p + ylim(0, 1.1*max(input_data_week$avg_over_wk))
  

  ggsave(filename,width = width/5, height = height/5, units = "mm", dpi=300)
}

#' Extracts the data relevant to the locations and files provided in tidy format
#'
#' @param locations A matrix of locations with headers "State, City, Site".
#' @param filenames A list of filenames to input
#' @return A tidy dataset with results specific to requested locations
reform_airdata <- function(locations=list(), filenames=list()) {


  intermediate_result = foreach (filename=filenames, .combine=rbind) %dopar% {
    rawdata = read.csv(filename,header=TRUE,blank.lines.skip = TRUE,na.strings="",stringsAsFactors=FALSE, skipNul = TRUE)

    matching_state_city = FALSE
    for (row in 1:nrow(locations)) {
      matching_state_city = matching_state_city | (rawdata$State.Name==locations[row,"State"] & rawdata$County.Name==locations[row,"City"] & !is.na(rawdata$Sample.Measurement) )
    }

    subpart = rawdata[ which(matching_state_city),]
    date_string_vec = paste(subpart$Date.Local,subpart$Time.Local)
    datetime_vec <- as.POSIXlt(date_string_vec)
    data_cache = data.frame("date" = datetime_vec, county=subpart$County.Name,  measurement=subpart$Sample.Measurement, stringsAsFactors = FALSE)
    data_cache
  }

  flog.info(" - extracted %s rows, %s columns from %s files [last date: %s] (intermediate: %s)", nrow(intermediate_result), ncol(intermediate_result), length(filenames), intermediate_result[nrow(intermediate_result)-1,1], colnames(intermediate_result))

  cleaned_result <- intermediate_result %>%
    na.omit() %>%
    group_by(date,county) %>%
    summarise(measurement = mean(measurement))

  flog.info(" - averaged data %s rows, %s columns (cleansed: %s)", nrow(cleaned_result), ncol(cleaned_result), colnames(cleaned_result))
  return(cleaned_result)
}

#' Get all data related to a specific gas for a set number of locations
#'
#' @param gas The ID of the gas desired
#' @param locations A matrix of locations with headers "State, City, Site".
#' @param data_dir A directory location with all the csv datafiles
#' @return A tidy dataset with date, location and value (of the measurement for that gas)
get_airdata_cache <- function(gas=NA,locations=NA,data_dir=NA) {
  # Load cached file (if exists), else generate
  hash <- digest(locations,algo="md5", serialize=F)
  cached_data_filename = paste(gas,"_",hash,".rds",sep = "")
  if (file.exists(cached_data_filename)) {
    flog.info("Found cache for %s gas [file: %s]",gas, cached_data_filename)
    start_time <- Sys.time()
    gas_compare <- readRDS(cached_data_filename)
    end_time <- Sys.time()
    flog.info("Cache load for %s gas took %0.2f seconds (ncol: %s, nrows: %s)",gas,end_time-start_time,ncol(gas_compare),nrow(gas_compare))
  }
  else {
    gas_compare <- NA
    input_filenames = get_hourly_airdata(gas=gas,dir=data_dir)
    flog.info("Failed to find cache for %s gas [file: %s]",gas, cached_data_filename)
    
    start_time <- Sys.time()
    gas_compare <- reform_airdata(locations=locations, filenames=input_filenames)
    end_time <- Sys.time()
    saveRDS(gas_compare, file = cached_data_filename)
    flog.info("Cache generation for %s gas took %0.2f seconds (ncol: %s, nrows: %s)",gas,end_time-start_time,ncol(gas_compare),nrow(gas_compare))
  }

  return(gas_compare)
}

#' Get all files specific to a gas located in a specific directory
#'
#' @param gas The ID of the gas desired
#' @param dir A directory location with all the csv datafiles
#' @return A list of files as requested
get_hourly_airdata <- function(gas=0000,dir=NA) {

  if (!is.na(dir)) {
    setwd(dir)
  }

  pattern = paste(".*_",gas,"_.*.csv",sep = "")
  filenames <- list.files(getwd(), pattern=pattern, full.names=TRUE)
}


##############################################
##############################################

#' Run a sample plot for Los Angeles and the Bronx for a few gasses
#'
main <- function() {
  #install_packages()
  prepare_environment(cores=14)


  locations = noquote(matrix(c("California","Los Angeles",1002,"New York","Bronx",0003),ncol=3,byrow=TRUE))
  #locations = noquote(matrix(c("California","Los Angeles",1002,"New York","New York",0135),ncol=3,byrow=TRUE))
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
