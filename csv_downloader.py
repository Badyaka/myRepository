import sys

import urllib2
import gzip
import StringIO

TIMEOUT = 60

# TODO change link for your needs
# URL = 'https://beta.casino-x.com/sa/serious_stats/csv/'

FEEDS = ['regs.csv', 'presents.csv', 'money.csv', 'games.csv', 'presents.csv']

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36'


def pack_cookie(cookies):
    return '; '.join(['%s=%s' % (k,v) for k,v in cookies.iteritems()])


def do_get(url, cookies=None):
    print 'GET', url, cookies
    headers = {'User-Agent': USER_AGENT, 'Cookie': pack_cookie(cookies)}
    request = urllib2.Request(url, headers=headers)
    return urllib2.urlopen(request, timeout=TIMEOUT)


def main():
    # TODO pass cookie through param or write manually
    cookies = {'sid': 'bf189fef24ba3309de1301494103c17a'}

    start_date = '2016-03-18'
    end_date = '2016-03-19'

    for feed in FEEDS:
        print 'Trying to load', feed
        # TODO also we can download file directly using "urllib.urlretrieve"

        url = '%s%s?start_date=%s&end_date=%s' % (URL, feed, start_date, end_date)
        response = do_get(url, cookies=cookies)
        print 'Response status:', response.getcode()

        meta = response.info()
        file_size = int(meta.getheaders('Content-Length')[0])
        print 'Download file with size', file_size

        gz_file = StringIO.StringIO()
        gz_file.write(response.read())
        gz_file.seek(0)

        data = gzip.GzipFile(fileobj=gz_file, mode='rb')
        for line in data.readlines():
            print line
            # TODO do something


if __name__ == '__main__':
    sys.exit(main())
