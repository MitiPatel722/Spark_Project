def dropduplicate_exclidingPK(dataframe,primarykeyColumn):
        dataframe_dropduplicate_expk = dataframe.dropDuplicates(subset=[
        col_name for col_name in dataframe.columns if col_name != primarykeyColumn])
        return dataframe_dropduplicate_expk
df = spark.sql("select * from miti_db.students")
df.show()
dropduplicate_exclidingPK(df,'student_id').show()
