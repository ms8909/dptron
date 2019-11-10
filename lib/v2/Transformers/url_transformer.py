from lib.v2.imports import *
from nltk.corpus import stopwords


class UrlTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    column = Param(Params._dummy(), "column", "column for transformation", typeConverter=TypeConverters.toString)

    def __init__(self, column='', base_url=''):
        super(UrlTransformer, self).__init__()

        self._setDefault(column=column)
        self.setColumn(column)

        self.stop_words = stopwords.words('english')
        self.base_url = base_url

    def getColumn(self):
        """
        Gets the value of withMean or its default value.
        """
        return self.getOrDefault(self.column)

    def setColumn(self, value):
        """
        Sets the value of :py:attr:`withStd`.
        """
        return self._set(column=value)

    def _transform(self, df):
        # df = df.withColumn(self.getColumn() + '_new',
        #                    self.udf_remove_stop_words(self.base_url)(
        #                        funct.trim(funct.lower(funct.col(self.getColumn()).cast("string")))))
        # df=df.drop(self.getColumn())
        print("inside transform")
        print(len(self.getColumn()))
        print(self.getColumn())
        df = df.withColumn(self.getColumn() ,
                           self.udf_remove_stop_words(self.base_url)(
                               funct.trim(funct.lower(funct.col(self.getColumn()).cast("string")))))

        return df

    def removing_stop_words(self, x, base_url):
        """
        url column and base_url is given and cleaned url is returned

        :param x: row on which cleaning is need to be performed
        :param base_url: Contains base_url
        :return: cleaned url
        """
        try:
            # If base_url param is empty figure out base_url using urllib
            if x is not None:
                if base_url == '':
                    base_url = urlparse(x)
                    base_url = base_url.netloc if base_url.scheme != '' else base_url.path.split("/")[0]

                res = x.replace("https://", "").replace("http://", "").replace(base_url, "")

                # fetch only alphabets ignore all special characters
                tokens = re.findall(r"[\w:']+", res)

                # remove duplicate words from url
                tokens = list(dict.fromkeys(tokens))

                # remove stop words from url
                elem = []
                if len(tokens) > 0:
                    elem = [word for word in tokens if word not in self.stop_words]

                # add base_url to the url
                elem.insert(0, base_url)
                new_url = '/'.join(elem)

                return new_url
        except Exception as e:
            logger.error(e)
            return x

    def udf_remove_stop_words(self, base_url):
        """
        a run function to create a udf function with default params
        :param base_url: contains base_url if any
        :return: cleaned url
        """
        return funct.udf(lambda x: self.removing_stop_words(x, base_url))
