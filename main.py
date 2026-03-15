from utils.analysis import payment_analysis, encoding_errors, aggregation_analysis

if __name__ == '__main__':
    payment_analysis()
    print('=' * 70)
    aggregation_analysis()
    encoding_errors()