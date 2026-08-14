class Dynamic():

    def dropColumns(self,df,column):
        df=df.drop(*column)
        return df
    