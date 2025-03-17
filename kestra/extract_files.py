from kestra import Kestra

if __name__ == "__main__":
    logger = Kestra.logger()
    
    logger.error("hello world!")