FROM rocker/shiny
MAINTAINER James Turtle Version 1.0
# Environment variables

# Add required libraries to the OS
RUN apt-get update && apt-get install -y \
	vim \
	ssh \
#	net-tools \
        apt-utils \
	unzip \
	libssl-dev \
	cron \
        cmake \
        git \
        g++ 
# add the shiny user
RUN    mkdir -p /home/shiny/lib /home/shiny/include && \
    echo "shiny ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/shiny && \
    chmod 0440 /etc/sudoers.d/shiny  

#installing the mongoc dependencies and driver
RUN apt-get install -y \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    libmongoc-1.0-0 \
    libbson-1.0 \
    libmariadb-client-lgpl-dev \
    libpq-dev \
    libnetcdf-dev

RUN cd ~ \
    && wget https://github.com/mongodb/mongo-c-driver/releases/download/1.6.2/mongo-c-driver-1.6.2.tar.gz \
    && tar xzf mongo-c-driver-1.6.2.tar.gz \
    && cd mongo-c-driver-1.6.2 \
    && ./configure --disable-automatic-init-and-cleanup \
    && make \
    && make install \
    && cd ~ \
    && rm mongo-c-driver-1.6.2.tar.gz \
    && rm -rf mongo-c-driver-1.6.2

#installing mongocxx driver - connects c++ to mongo
RUN cd ~ \
    && wget https://github.com/mongodb/mongo-cxx-driver/archive/r3.1.1.tar.gz \
    && tar -xzf r3.1.1.tar.gz \
    && cd mongo-cxx-driver-r3.1.1/build \
    && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr/local .. \
    && make EP_mnmlstc_core \
    && make \
    && make install \
    && cd ~ \
    && rm r3.1.1.tar.gz \
    && rm -rf mongo-cxx-driver-r3.1.1


# remove after debugging
COPY pw.txt /root
RUN cat /root/pw.txt | chpasswd
RUN rm /root/pw.txt

# Setup services
COPY init_setup.sh /home/shiny
RUN chmod 755 /home/shiny/init_setup.sh
COPY setup.cron /home/devel/setup.cron

# Modify shiny-server.conf as needed
# COPY shiny-server.conf /etc/shiny-server/shiny-server.conf

# Modify the /etc/init.d/shiny-server file
# COPY shiny-server /etc/init.d/shiny-server

# Download latest R source
WORKDIR /home/shiny
COPY installRpackages.R /home/shiny
RUN chown -R shiny:shiny /home/shiny

# copy NOAA_proc source files into a container directory
RUN mkdir -p /home/source/temp/
COPY source/* /home/source/
WORKDIR /home/source
RUN ls
# compile mongodbFunctions.c to an R-accessible shared library
RUN gcc -g -c -fPIC -I /usr/local/include/libmongoc-1.0/ -I /usr/local/include/libbson-1.0/ mongodbFunctions.c \
    && R CMD SHLIB /usr/local/lib/libmongoc-1.0.so /usr/local/lib/libbson-1.0.so mongodbFunctions.o \
    && gcc -g -I /usr/local/include/libmongoc-1.0/ -I /usr/local/include/libbson-1.0/ -Wl,-rpath-link,/usr/local/lib/R/lib/ test.c mongodbFunctions.so -o test.out

#   Add required R libraries 
RUN Rscript /home/shiny/installRpackages.R
RUN ldconfig

# Initiate crontab
RUN crontab -l -u devel | cat /home/devel/setup.cron | crontab - -u devel
# RUN chown -R shiny:shiny /srv/shiny-server/shiny-bSIR
# ENV HOME /home/shiny
# ENTRYPOINT [ "sh", "-c", "/home/shiny/init_setup.sh" ]
