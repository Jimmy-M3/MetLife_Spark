CREATE  TABLE if not exists LDZ.LDZ_LA_ZPCMPF_TEST(
    hist_id decimal(20,0) ,
    zactcode varchar(100) ,
    billfreq varchar(100) ,
    crtable varchar(100) ,
    cnttype varchar(100) ,
    zplandesc varchar(100),
    CONSTRAINT my_pk PRIMARY KEY (hist_id));