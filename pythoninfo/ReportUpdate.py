from sqlalchemy import create_engine, text
from datetime import datetime
import calendar
import ollamajsonfilter

def UpdateRateingDayReport(fileinfo):
    def get_record_countRateingDayReport(table_name, agent, date,service, engine):
        """
        Get comprehensive lead statistics for a given agent and date.
        Returns a tuple: (total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue)
        agent = :agent 
        """
        sql = f"""
        SELECT 
            COUNT(*) AS total_leads,
            COUNT(CASE WHEN leadrating != 'UNSOLD' THEN 1 END) AS non_unsold_leads,
            COUNT(CASE WHEN leadrating = 'UNSOLD' THEN 1 END) AS unsold_leads,
            SUM(evaluatortimer_int) AS total_evaluator_timer,
            SUM(CASE WHEN leadrating != 'UNSOLD' THEN LeadRevenue99_float ELSE 0 END) AS total_lead_revenue
        FROM {table_name} 
        WHERE Created_date = :date
          """
        if (agent!="ALL"):
            sql+=" AND agent = :agent"
        if (service!="ALL"):
            sql+=" AND Service = :Service"

        print(sql)
        from sqlalchemy import text
        
        stmt = text(sql)
        with engine.begin() as conn:
            result = conn.execute(stmt, {"agent": agent, "date": date,"Service":service})
            row = result.first()
            
            # Handle potential NULL values
            total_leads = row.total_leads or 0
            non_unsold_leads = row.non_unsold_leads or 0
            unsold_leads = row.unsold_leads or 0
            total_evaluator_timer = row.total_evaluator_timer or 0
            total_lead_revenue = row.total_lead_revenue or 0.0
        
        # Print results for debugging
        print(f"Lead statistics for {agent} on {date}:")
        print(f"  Total leads: {total_leads}")
        print(f"  Non-unsold leads: {non_unsold_leads}")
        print(f"  Unsold leads: {unsold_leads}")
        print(f"  Total evaluator timer: {total_evaluator_timer}")
        print(f"  Total lead revenue: ${total_lead_revenue:.2f}")
        
        return total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue

    def insert_single_agentRatingDayReport(rater, processed, sold, revenue, closing_rate, value_per_opportunity, value_per_lead, date_rating, day_of_week, callTime, Vertical,engine):
        """Insert a single agent rating record - SQLAlchemy 2.0 compatible with update if key exists"""
        try:
            from sqlalchemy import text
            print(Vertical)
            
            with engine.begin() as connection:  # Use begin() for auto-commit
                # Use INSERT ... ON DUPLICATE KEY UPDATE for MySQL
                # This will insert if the record doesn't exist, or update if it does
                upsert_query = text("""
                    INSERT INTO day_ratings (Rater, Processed, Sold, Revenue, ClosingRate, ValuePerOpportunity, ValuePerLead, DateRating, DayOfWeek, AverageCallTime,vertical) 
                    VALUES (:rater, :processed, :sold, :revenue, :closing_rate, :value_per_opportunity, :value_per_lead, :date_rating, :day_of_week, :AverageCallTime, :Vertical)
                    ON DUPLICATE KEY UPDATE
                        Processed = VALUES(Processed),
                        Sold = VALUES(Sold),
                        Revenue = VALUES(Revenue),
                        ClosingRate = VALUES(ClosingRate),
                        ValuePerOpportunity = VALUES(ValuePerOpportunity),
                        ValuePerLead = VALUES(ValuePerLead),
                        AverageCallTime = VALUES(AverageCallTime)
                """)
                
                connection.execute(upsert_query, {
                    'rater': rater,
                    'processed': processed,
                    'sold': sold,
                    'revenue': revenue,
                    'closing_rate': closing_rate,
                    'value_per_opportunity': value_per_opportunity,
                    'value_per_lead': value_per_lead,
                    'date_rating': date_rating,
                    'day_of_week': day_of_week,
                    'AverageCallTime': callTime,
                    'Vertical': Vertical
                })
                
                print(f"Successfully inserted/updated record for {rater} on {date_rating}")
                
        except Exception as e:
            print(f"Error inserting/updating record: {e}")

    MYSQL_URI = "mysql+pymysql://admin:RQApoaNQ@mysql-199933-0.cloudclusters.net:10033/working_db"
    engine = create_engine(MYSQL_URI)
    count=get_record_countRateingDayReport("myTable",fileinfo[0][1],fileinfo[0][2],fileinfo[0][3],engine)
    date_str = fileinfo[0][2]
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    if (count[0]==0):
        insert_single_agentRatingDayReport(fileinfo[0][1], count[0], count[1], count[4], 0, 0, 0,fileinfo[0][2],dt.strftime('%A'),0,fileinfo[0][3] ,engine)
    else:
        if (count[1]==0):
            insert_single_agentRatingDayReport(fileinfo[0][1], count[0], count[1], count[4], count[1]/( count[0]+ count[1]), count[4]/count[0],0,fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)
        else:
            insert_single_agentRatingDayReport(fileinfo[0][1], count[0], count[1], count[4], count[1]/( count[0]+ count[1]), count[4]/count[0],count[4]/count[1],fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)
    print(fileinfo[0][3])



def UpdateRateingweekReport(fileinfo):
    def get_record_countRateingWeekReport(table_name, agent, date,service, engine):
        """
        Get comprehensive lead statistics for a given agent and date.
        Returns a tuple: (total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue)
        """
        sql = f"""
        SELECT 
            COUNT(*) AS total_leads,
            COUNT(CASE WHEN leadrating != 'UNSOLD' THEN 1 END) AS non_unsold_leads,
            COUNT(CASE WHEN leadrating = 'UNSOLD' THEN 1 END) AS unsold_leads,
            SUM(evaluatortimer_int) AS total_evaluator_timer,
            SUM(CASE WHEN leadrating != 'UNSOLD' THEN LeadRevenue99_float ELSE 0 END) AS total_lead_revenue
        FROM {table_name} 
        WHERE agent = :agent 
          AND Created_date >= DATE_SUB(:date, INTERVAL 7 DAY)
          AND Created_date <= :date
        """
        if (service!="ALL"):
            sql+=" AND Service = :Service"
        
        print(sql)
        from sqlalchemy import text
        
        stmt = text(sql)
        with engine.begin() as conn:
            result = conn.execute(stmt, {"agent": agent, "date": date,"Service":service})
            row = result.first()
            
            # Handle potential NULL values
            total_leads = row.total_leads or 0
            non_unsold_leads = row.non_unsold_leads or 0
            unsold_leads = row.unsold_leads or 0
            total_evaluator_timer = row.total_evaluator_timer or 0
            total_lead_revenue = row.total_lead_revenue or 0.0
        
        # Print results for debugging
        print(f"Lead statistics for {agent} on {date}:")
        print(f"  Total leads: {total_leads}")
        print(f"  Non-unsold leads: {non_unsold_leads}")
        print(f"  Unsold leads: {unsold_leads}")
        print(f"  Total evaluator timer: {total_evaluator_timer}")
        print(f"  Total lead revenue: ${total_lead_revenue:.2f}")
        
        return total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue

    def insert_single_agentRateingWeekReport(rater, processed, sold, revenue, closing_rate, value_per_opportunity, value_per_lead, date_rating, day_of_week, callTime, Vertical,engine):
        """Insert a single agent rating record - SQLAlchemy 2.0 compatible with update if key exists"""
        try:
            from sqlalchemy import text
            
            with engine.begin() as connection:  # Use begin() for auto-commit
                # Use INSERT ... ON DUPLICATE KEY UPDATE for MySQL
                # This will insert if the record doesn't exist, or update if it does
                upsert_query = text("""
                    INSERT INTO Week_ratings (Rater, Processed, Sold, Revenue, ClosingRate, ValuePerOpportunity, ValuePerLead, WeekRating, DayOfWeek, AverageCallTime,vertical) 
                    VALUES (:rater, :processed, :sold, :revenue, :closing_rate, :value_per_opportunity, :value_per_lead, :date_rating, :day_of_week, :callTime,:vertical)
                    ON DUPLICATE KEY UPDATE
                        Processed = VALUES(Processed),
                        Sold = VALUES(Sold),
                        Revenue = VALUES(Revenue),
                        ClosingRate = VALUES(ClosingRate),
                        ValuePerOpportunity = VALUES(ValuePerOpportunity),
                        ValuePerLead = VALUES(ValuePerLead),
                        AverageCallTime = VALUES(AverageCallTime)
                """)
                
                connection.execute(upsert_query, {
                    'rater': rater,
                    'processed': processed,
                    'sold': sold,
                    'revenue': revenue,
                    'closing_rate': closing_rate,
                    'value_per_opportunity': value_per_opportunity,
                    'value_per_lead': value_per_lead,
                    'date_rating': date_rating,
                    'day_of_week': day_of_week,
                    'callTime': callTime,
                   'vertical': Vertical,
                })
                
                print(f"Successfully inserted/updated record for {rater} on {date_rating}")
                
        except Exception as e:
            print(f"Error inserting/updating record: {e}")

    MYSQL_URI = "mysql+pymysql://admin:RQApoaNQ@mysql-199933-0.cloudclusters.net:10033/working_db"
    engine = create_engine(MYSQL_URI)
    count=get_record_countRateingWeekReport("myTable",fileinfo[0][1],fileinfo[0][2],fileinfo[0][3],engine)
    date_str = fileinfo[0][2]
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    if (count[0]==0):
        insert_single_agentRateingWeekReport(fileinfo[0][1], count[0], count[1], count[4], 0, 0, 0,fileinfo[0][2],dt.strftime('%A'),0,fileinfo[0][3] ,engine)
    else:
        if (count[1]==0):
            insert_single_agentRateingWeekReport(fileinfo[0][1], count[0], count[1], count[4], count[1]/( count[0]+ count[1]), count[4]/count[0],0,fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)
        else:
            insert_single_agentRateingWeekReport(fileinfo[0][1], count[0], count[1], count[4], count[1]/( count[0]+ count[1]), count[4]/count[0],count[4]/count[1],fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)





def UpdateTypeDayReport(fileinfo):

    def get_record_countRateingDayReport(table_name, LeadType, date,service, engine):
        """
        Get comprehensive lead statistics for a given agent and date.
        Returns a tuple: (total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue)
        """
        sql = f"""
        SELECT 
            COUNT(*) AS total_leads,
            COUNT(CASE WHEN leadrating != 'UNSOLD' THEN 1 END) AS non_unsold_leads,
            COUNT(CASE WHEN leadrating = 'UNSOLD' THEN 1 END) AS unsold_leads,
            SUM(evaluatortimer_int) AS total_evaluator_timer,
            SUM(CASE WHEN leadrating != 'UNSOLD' THEN LeadRevenue99_float ELSE 0 END) AS total_lead_revenue
        FROM {table_name} 
        WHERE Created_date = :date
          """
        if (service!="ALL"):
            sql+=" AND Service = :Service"
        if (LeadType!="ALL"):
            sql+=" AND LeadType = :LeadType"

        print(sql)
        from sqlalchemy import text
        
        stmt = text(sql)
        with engine.begin() as conn:
            result = conn.execute(stmt, {"LeadType": LeadType, "date": date,"Service":service})
            row = result.first()
            
            # Handle potential NULL values
            total_leads = row.total_leads or 0
            non_unsold_leads = row.non_unsold_leads or 0
            unsold_leads = row.unsold_leads or 0
            total_evaluator_timer = row.total_evaluator_timer or 0
            total_lead_revenue = row.total_lead_revenue or 0.0
        
        # Print results for debugging
        print(f"Lead statistics for {LeadType} on {date}:")
        print(f"  Total leads: {total_leads}")
        print(f"  Non-unsold leads: {non_unsold_leads}")
        print(f"  Unsold leads: {unsold_leads}")
        print(f"  Total evaluator timer: {total_evaluator_timer}")
        print(f"  Total lead revenue: ${total_lead_revenue:.2f}")
        
        return total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue

    def insert_single_agentTypeDayReport(LeadSource, processed, sold, revenue, closing_rate, value_per_opportunity, value_per_lead, date_rating, day_of_week, callTime, Vertical,engine):
        """Insert a single agent rating record - SQLAlchemy 2.0 compatible with update if key exists"""
        try:
            from sqlalchemy import text
            
            with engine.begin() as connection:  # Use begin() for auto-commit
                # Use INSERT ... ON DUPLICATE KEY UPDATE for MySQL
                # This will insert if the record doesn't exist, or update if it does
                upsert_query = text("""
                    INSERT INTO lead_type_day (LeadSource, Processed, Sold, Revenue, ClosingRate, ValuePerOpportunity, ValuePerLead, DayRating, DayOfWeek, AverageCallTime,vertical) 
                    VALUES (:LeadSource, :processed, :sold, :revenue, :closing_rate, :value_per_opportunity, :value_per_lead, :date_rating, :day_of_week, :callTime,:vertical)
                    ON DUPLICATE KEY UPDATE
                        Processed = VALUES(Processed),
                        Sold = VALUES(Sold),
                        Revenue = VALUES(Revenue),
                        ClosingRate = VALUES(ClosingRate),
                        ValuePerOpportunity = VALUES(ValuePerOpportunity),
                        ValuePerLead = VALUES(ValuePerLead),
                        AverageCallTime = VALUES(AverageCallTime)
                """)
                
                connection.execute(upsert_query, {
                    'LeadSource': LeadSource,
                    'processed': processed,
                    'sold': sold,
                    'revenue': revenue,
                    'closing_rate': closing_rate,
                    'value_per_opportunity': value_per_opportunity,
                    'value_per_lead': value_per_lead,
                    'date_rating': date_rating,
                    'day_of_week': day_of_week,
                    'callTime': callTime,
                   'vertical': Vertical,

                })
                
                print(f"Successfully inserted/updated record for {LeadSource} on {date_rating}")
                
        except Exception as e:
            print(f"Error inserting/updating record: {e}")

    MYSQL_URI = "mysql+pymysql://admin:RQApoaNQ@mysql-199933-0.cloudclusters.net:10033/working_db"
    engine = create_engine(MYSQL_URI)
    date_str = fileinfo[0][2]
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    count=get_record_countRateingDayReport("myTable",fileinfo[0][1],fileinfo[0][2],fileinfo[0][3],engine)
    if (count[0]==0):
        insert_single_agentTypeDayReport(fileinfo[0][1], count[0], count[1], count[4], 0, 0, 0,fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)
    else:
        if (count[1]==0):
            insert_single_agentTypeDayReport(fileinfo[0][1], count[0], count[1], count[4], count[1]/( count[0]+ count[1]), count[4]/count[0],0,fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)
        else:
            insert_single_agentTypeDayReport(fileinfo[0][1], count[0], count[1], count[4], count[1]/( count[0]+ count[1]), count[4]/count[0],count[4]/count[1],fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)







def UpdateTypeWeekReport(fileinfo):

    def get_record_countRateingDayReport(table_name, LeadType, date,service, engine):
        """
        Get comprehensive lead statistics for a given agent and date.
        Returns a tuple: (total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue)
        """
        sql = f"""
        SELECT 
            COUNT(*) AS total_leads,
            COUNT(CASE WHEN leadrating != 'UNSOLD' THEN 1 END) AS non_unsold_leads,
            COUNT(CASE WHEN leadrating = 'UNSOLD' THEN 1 END) AS unsold_leads,
            SUM(evaluatortimer_int) AS total_evaluator_timer,
            SUM(CASE WHEN leadrating != 'UNSOLD' THEN LeadRevenue99_float ELSE 0 END) AS total_lead_revenue
        FROM {table_name} 
        WHERE Created_date = :date
          AND Created_date >= DATE_SUB(:date, INTERVAL 7 DAY)
          AND Created_date <= :date
          """
        if (service!="ALL"):
            sql+=" AND Service = :Service"
        if (LeadType!="ALL"):
            sql+=" AND LeadType = :LeadType"

        print(sql)
        from sqlalchemy import text
        
        stmt = text(sql)
        with engine.begin() as conn:
            result = conn.execute(stmt, {"LeadType": LeadType, "date": date,"Service":service})
            row = result.first()
            
            # Handle potential NULL values
            total_leads = row.total_leads or 0
            non_unsold_leads = row.non_unsold_leads or 0
            unsold_leads = row.unsold_leads or 0
            total_evaluator_timer = row.total_evaluator_timer or 0
            total_lead_revenue = row.total_lead_revenue or 0.0
        
        # Print results for debugging
        print(f"Lead statistics for {LeadType} on {date}:")
        print(f"  Total leads: {total_leads}")
        print(f"  Non-unsold leads: {non_unsold_leads}")
        print(f"  Unsold leads: {unsold_leads}")
        print(f"  Total evaluator timer: {total_evaluator_timer}")
        print(f"  Total lead revenue: ${total_lead_revenue:.2f}")
        
        return total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue

    def insert_single_agentTypeDayReport(LeadSource, processed, sold, revenue, closing_rate, value_per_opportunity, value_per_lead, date_rating, day_of_week, callTime, Vertical,engine):
        """Insert a single agent rating record - SQLAlchemy 2.0 compatible with update if key exists"""
        try:
            from sqlalchemy import text
            
            with engine.begin() as connection:  # Use begin() for auto-commit
                # Use INSERT ... ON DUPLICATE KEY UPDATE for MySQL
                # This will insert if the record doesn't exist, or update if it does
                upsert_query = text("""
                    INSERT INTO Week_ratings (LeadSource, Processed, Sold, Revenue, ClosingRate, ValuePerOpportunity, ValuePerLead, WeekRating, DayOfWeek, AverageCallTime,vertical) 
                    VALUES (:LeadSource, :processed, :sold, :revenue, :closing_rate, :value_per_opportunity, :value_per_lead, :date_rating, :day_of_week, :callTime,:vertical)
                    ON DUPLICATE KEY UPDATE
                        Processed = VALUES(Processed),
                        Sold = VALUES(Sold),
                        Revenue = VALUES(Revenue),
                        ClosingRate = VALUES(ClosingRate),
                        ValuePerOpportunity = VALUES(ValuePerOpportunity),
                        ValuePerLead = VALUES(ValuePerLead),
                        AverageCallTime = VALUES(AverageCallTime)
                """)
                
                connection.execute(upsert_query, {
                    'LeadSource': LeadSource,
                    'processed': processed,
                    'sold': sold,
                    'revenue': revenue,
                    'closing_rate': closing_rate,
                    'value_per_opportunity': value_per_opportunity,
                    'value_per_lead': value_per_lead,
                    'date_rating': date_rating,
                    'day_of_week': day_of_week,
                    'callTime': callTime,
                   'vertical': Vertical,

                })
                
                print(upsert_query)

                print(f"Successfully inserted/updated record for {LeadSource} on {date_rating}")
                
        except Exception as e:
            print(f"Error inserting/updating record: {e}")

    MYSQL_URI = "mysql+pymysql://admin:RQApoaNQ@mysql-199933-0.cloudclusters.net:10033/working_db"
    engine = create_engine(MYSQL_URI)
    date_str = fileinfo[0][2]
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    count=get_record_countRateingDayReport("myTable",fileinfo[0][1],fileinfo[0][2],fileinfo[0][3],engine)
    if (count[0]==0):
        insert_single_agentTypeDayReport(fileinfo[0][1], count[0], count[1], count[4], 0, 0, 0,fileinfo[0][2],dt.strftime('%A'),0,fileinfo[0][3] ,engine)
    else:
        if (count[1]==0):
            insert_single_agentTypeDayReport(fileinfo[0][1], count[0], count[1], count[4], count[1]/( count[0]+ count[1]), count[4]/count[0],0,fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)
        else:
            insert_single_agentTypeDayReport(fileinfo[0][1], count[0], count[1], count[4], count[1]/( count[0]+ count[1]), count[4]/count[0],count[4]/count[1],fileinfo[0][2],dt.strftime('%A'),count[3]/count[0],fileinfo[0][3] ,engine)









def TranscribeInvaid(fileinfo):

    def get_record_countRateingDayReport(table_name, id_, engine):
        """
        Get comprehensive lead statistics for a given agent and date.
        Returns a tuple: (total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue)
        """
        sql = f"""
        SELECT 

dataUpdated as dataUpdated,
Service as Service,
Created_date as Created_date,
Contact as Contact,
Company as Company,
CompanyType as CompanyType,



Zip as Zip,
City as City,
County as County,
State as State,
MSA as MSA,
Phone as Phone,
Email as Email,
ReviewLink as ReviewLink,




TypeOfMaterials as TypeOfMaterials,
OnePurgeService as OnePurgeService,
MobileOrOffsite as MobileOrOffsite,
BusinessOrResidence as BusinessOrResidence,
Rater as Rater,
Rating as Rating,
Subrating as Subrating,
RatingDescription as RatingDescription,
DatRated as DatRated,
Contractors as Contractors,
LeadSource as LeadSource,
LeadType as LeadType,
LeadPage as LeadPage,





LeadSourceType as LeadSourceType,
ContainerQty as ContainerQty,
ContainerType as ContainerType,
NumBoxes as NumBoxes,
NumPages as NumPages,
NumPounds as NumPounds,
LeadNoteToPartner as LeadNoteToPartner,
LeadRevenue99_float as LeadRevenue99_float,
KnownCompany as KnownCompany,
Fortune1000 as Fortune1000,
transcriptAudioXnadmin as transcriptAudioXnadmin,
fileAudioXnadmin as fileAudioXnadmin,
leadid4 as leadid4,



leadtype7 as leadtype7,
leadsource2 as leadsource2,
leadrating as leadrating,
leadtype1 as leadtype1,
evaluatortimer_int as evaluatortimer_int,
agent as agent,



            transcriptAudioXnadmin as transcript
        FROM {table_name} 
        WHERE id = :id
          """
        print(sql)
        from sqlalchemy import text
        
        stmt = text(sql)
        with engine.begin() as conn:
            result = conn.execute(stmt, {"id": id_})
            row = result.first()

            # Handle potential NULL values
            transcript = row.transcript or 0

        return row


    def get_record_countRateingDayReport_prompt(table_name, id_, engine):
        """
        Get comprehensive lead statistics for a given agent and date.
        Returns a tuple: (total_leads, non_unsold_leads, unsold_leads, total_evaluator_timer, total_lead_revenue)
        """
        sql = f"""
        SELECT 
            prompt as prompt
        FROM {table_name} 
        WHERE id = :id
          """

        print(sql)
        from sqlalchemy import text
        
        stmt = text(sql)
        with engine.begin() as conn:
            result = conn.execute(stmt, {"id": id_})
            row = result.first()
            
            # Handle potential NULL values
            prompt = row.prompt or 0

        
        return prompt



    def insert_single_agentTypeDayReport(row, dictionary,prompt,engine):
        """Insert a single agent rating record - SQLAlchemy 2.0 compatible with update if key exists"""
        try:
            from sqlalchemy import text
            
            with engine.begin() as connection:  # Use begin() for auto-commit
                # Use INSERT ... ON DUPLICATE KEY UPDATE for MySQL
                # This will insert if the record doesn't exist, or update if it does



                upsert_query = text("""
                    INSERT INTO AICallTable (Service,Created_date,Contact,Company,CompanyType,Zip,City,County,State,MSA,Phone,
                        Email,ReviewLink,agent,evaluatortimer_int,leadtype1,leadrating,leadsource2,leadtype7,leadid4,fileAudioXnadmin,Fortune1000,KnownCompany,
                        LeadRevenue99_float,LeadNoteToPartner,NumPounds,NumPages,NumBoxes,ContainerType,ContainerQty,LeadSourceType,
                        LeadPage,LeadType,LeadSource,Contractors,DatRated,RatingDescription,Subrating,Rating,Rater,BusinessOrResidence,MobileOrOffsite,OnePurgeService,TypeOfMaterials,transcriptAudioXnadmin,
                        Invalid,Quoted,Chatworked,choise,valid,prompts,Referred) 
                    VALUES (:Service,:Created_date,:Contact,:Company,:CompanyType,:Zip,:City,:County,:State,:MSA,:Phone,
                        :Email,:ReviewLink,:agent,:evaluatortimer_int,:leadtype1,:leadrating,:leadsource2,:leadtype7,:leadid4,:fileAudioXnadmin,
                        :Fortune1000,:KnownCompany,:LeadRevenue99_float,:LeadNoteToPartner,:NumPounds,:NumPages,:NumBoxes,:ContainerType,:ContainerQty,:LeadSourceType,
                        :LeadPage,:LeadType,:LeadSource,:Contractors,:DatRated,:RatingDescription,:Subrating,:Rating,:Rater,:BusinessOrResidence,:MobileOrOffsite,:OnePurgeService,:TypeOfMaterials,:transcriptAudioXnadmin,
                        :Invalid,:Quoted,:Chatworked,:choise,:valid,:prompts,:Referred)
                    ON DUPLICATE KEY UPDATE
                        Service = VALUES(Service)
                """)
                
                connection.execute(upsert_query, {
                    'Service': row.Service,'Created_date': row.Created_date,'Contact': row.Contact,'Company': row.Company,'CompanyType': row.CompanyType,'Zip': row.Zip,
                    'City': row.City,'County': row.County,'State': row.State,'MSA': row.MSA,'Phone': row.Phone,'Email': row.Email,
                    'ReviewLink': row.ReviewLink,'agent':row.agent,"evaluatortimer_int":row.evaluatortimer_int,"leadtype1":row.leadtype1,"leadrating":row.leadrating,"leadsource2":row.leadsource2,
                    "leadtype7":row.leadtype7,"leadid4":row.leadid4,"fileAudioXnadmin":row.fileAudioXnadmin,"Fortune1000":row.Fortune1000,"KnownCompany":row.KnownCompany,"LeadRevenue99_float":row.LeadRevenue99_float,
                    "LeadNoteToPartner":row.LeadNoteToPartner,"NumPounds":row.NumPounds,"NumPages":row.NumPages,"NumBoxes":row.NumBoxes,"ContainerType":row.ContainerType,"ContainerQty":row.ContainerQty,"LeadSourceType":row.LeadSourceType,
                    "LeadPage":row.LeadPage,"LeadType":row.LeadType,"LeadSource":row.LeadSource,"Contractors":row.Contractors,"DatRated":row.DatRated,"RatingDescription":row.RatingDescription,"Subrating":row.Subrating,
                    "Rating":row.Rating,"Rater":row.Rater,"BusinessOrResidence":row.BusinessOrResidence,"MobileOrOffsite":row.MobileOrOffsite,"OnePurgeService":row.OnePurgeService,"TypeOfMaterials":row.TypeOfMaterials,"transcriptAudioXnadmin":row.transcriptAudioXnadmin,
                    "Invalid":str(dictionary['Invalid']),"Quoted":str(dictionary['Quoted']),"Chatworked":str(dictionary['raw_response']),"choise":str( dictionary['choise'] ),
                    "valid":str(dictionary['valid']),"prompts":prompt,"Referred":str(dictionary['Referred'])



                })
                
                print(upsert_query)
                
        except Exception as e:
            print(f"Error inserting/updating record: {e}")

    MYSQL_URI = "mysql+pymysql://admin:RQApoaNQ@mysql-199933-0.cloudclusters.net:10033/working_db"
    engine = create_engine(MYSQL_URI)
    count=get_record_countRateingDayReport("myTable",fileinfo[0][1],engine)
    prompt=get_record_countRateingDayReport_prompt("prompts",fileinfo[0][2],engine)

    array=[count.transcript,prompt,fileinfo[0][3],fileinfo[0][4]]
    output=ollamajsonfilter.getready(array)
    print()
    print()
    print()
    print(count)
    print()
    print()
    print(output)
    insert_single_agentTypeDayReport(count, output,prompt,engine)
    print(fileinfo)












