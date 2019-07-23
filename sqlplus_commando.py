#!/usr/bin/env python
# encoding: UTF-8

from __future__ import with_statement
import re
import os.path
import datetime
import subprocess
import HTMLParser


class SqlplusCommando(object):

    CATCH_ERRORS = 'WHENEVER SQLERROR EXIT SQL.SQLCODE;\nWHENEVER OSERROR EXIT 9;\n'
    COMMIT_COMMAND = b'\nCOMMIT;'#\nEXIT;\n'
    EXIT_COMMAND = b'\nEXIT;'
    ISO_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, configuration=None, port=None,
                 hostname=None, database=None,
                 username=None, password=None,
                 encoding='UTF-8', cast=False):
        if hostname and database and username and password:
            self.hostname = hostname
            self.database = database
            self.username = username
            self.password = password
            self.port     = port
        elif configuration:
            self.hostname = configuration['database_host']
            self.database = configuration['database_name']
            self.username = configuration['database_user']
            self.password = configuration['database_password']
            self.port     = configuration['database_port']
            self.version_table = configuration['database_version_table']
        else:
            raise SqlplusException('Missing database configuration')
        self.encoding = encoding
        self.cast = cast

    def run_query(self, query, parameters={}, cast=True, check_errors=True):
        if parameters:
            query = self._process_parameters(query, parameters)
        # query = self.CATCH_ERRORS + query
        session = subprocess.Popen(['sqlplus', '-S', '-L', '-M', 'HTML ON',
                                    self._get_connection_url()],
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        session.stdin.write(query.encode(self.encoding))

        output, _ = session.communicate(self.COMMIT_COMMAND + self.EXIT_COMMAND)
        code = session.returncode
        if code != 0:
            parsed = SqlplusErrorParser.parse(output.decode('utf-8'))
            raise SqlplusException(parsed, query)
        elif output:
            result = SqlplusResultParser.parse(output.decode('utf-8'), cast=cast, check_errors=check_errors)
            return result

    def run_script(self, script, cast=True, check_errors=True):
        if not os.path.isfile(script):
            raise SqlplusException("Script '%s' was not found" % script)
        query = "@%s\n" % script
        return self.run_query(query=query, cast=cast, check_errors=check_errors)

    def _get_connection_url(self):
        return "%s/%s@//%s:%s/%s" % \
               (self.username, self.password, self.hostname, self.port, self.database)

    @staticmethod
    def _process_parameters(query, parameters):
        if not parameters:
            return query
        if isinstance(parameters, (list, tuple)):
            parameters = tuple(SqlplusCommando._format_parameters(parameters))
        elif isinstance(parameters, dict):
            values = SqlplusCommando._format_parameters(parameters.values())
            parameters = dict(zip(parameters.keys(), values))
        return query % parameters

    @staticmethod
    def _format_parameters(parameters):
        return [SqlplusCommando._format_parameter(param) for
                param in parameters]

    @staticmethod
    def _format_parameter(parameter):
        if isinstance(parameter, (int, long, float)):
            return str(parameter)
        elif isinstance(parameter, (str, unicode)):
            return "'%s'" % SqlplusCommando._escape_string(parameter)
        elif isinstance(parameter, datetime.datetime):
            return "'%s'" % parameter.strftime(SqlplusCommando.ISO_FORMAT)
        elif isinstance(parameter, list):
            return "(%s)" % ', '.join([SqlplusCommando._format_parameter(e)
                                       for e in parameter])
        elif parameter is None:
            return "NULL"
        else:
            raise SqlplusException("Type '%s' is not managed as a query parameter" %
                                   parameter.__class__.__name__)

    @staticmethod
    def _escape_string(string):
        return string.replace("'", "''")


class SqlplusResultParser(HTMLParser.HTMLParser):

    DATE_FORMAT = '%d/%m/%y %H:%M:%S'
    REGEXP_ERRORS = ('^unknown.*$|^warning.*$|^error.*$|^.*LCD-[0-9]{5}.*$|^.*NCR-[0-9]{5}.*$|^.*LRM-[0-9]{5}.*$|^.*IMG-[0-9]{5}.*$|^.*PLW-[0-9]{5}.*$|^.*NZE-[0-9]{5}.*$|^.*CLSR-[0-9]{5}.*$|^.*CLST-[0-9]{5}.*$|^.*LFI-[0-9]{5}.*$|^.*PROC-[0-9]{5}.*$|^.*UDI-[0-9]{5}.*$|^.*QSM-[0-9]{5}.*$|^.*CLSS-[0-9]{5}.*$|^.*PCC-[0-9]{5}.*$|^.*OCI-[0-9]{5}.*$|^.*DIA-[0-9]{5}.*$|^.*LSX-[0-9]{5}.*$|^.*AMD-[0-9]{5}.*$|^.*SQL-[0-9]{5}.*$|^.*SQL*Loader-[0-9]{5}.*$|^.*ORA-[0-9]{5}.*$|^.*IMP-[0-9]{5}.*$|^.*UDE-[0-9]{5}.*$|^.*DRG-[0-9]{5}.*$|^.*NID-[0-9]{5}.*$|^.*PLS-[0-9]{5}.*$|^.*EVM-[0-9]{5}.*$|^.*EXP-[0-9]{5}.*$|^.*PROT-[0-9]{5}.*$|^.*NNF-[0-9]{5}.*$|^.*PGA-[0-9]{5}.*$|^.*NNC-[0-9]{5}.*$|^.*O2U-[0-9]{5}.*$|^.*NNL-[0-9]{5}.*$|^.*RMAN-[0-9]{5}.*$|^.*PGU-[0-9]{5}.*$|^.*VID-[0-9]{5}.*$|^.*NPL-[0-9]{5}.*$|^.*AUD-[0-9]{5}.*$|^.*PCB-[0-9]{5}.*$|^.*DBV-[0-9]{5}.*$|^.*NMP-[0-9]{5}.*$|^.*O2F-[0-9]{5}.*$|^.*KUP-[0-9]{5}.*$|^.*CLSD-[0-9]{5}.*$|^.*O2I-[0-9]{5}.*$|^.*LPX-[0-9]{5}.*$|^.*CRS-[0-9]{5}.*$|^.*DGM-[0-9]{5}.*$|^.*TNS-[0-9]{5}.*$|^.*NNO-[0-9]{5}.*$')
    CASTS = (
        (r'-?\d+', int),
        (r'-?\d*,?\d*([Ee][+-]?\d+)?', lambda f: float(f.replace(',', '.'))),
        (r'\d\d/\d\d/\d\d \d\d:\d\d:\d\d,\d*',
         lambda d: datetime.datetime.strptime(d[:17],
                                              SqlplusResultParser.DATE_FORMAT)),
        (r'NULL', lambda d: None),
    )

    def __init__(self, cast):
        HTMLParser.HTMLParser.__init__(self)
        self.cast = cast
        self.active = False
        self.result = []
        self.fields = []
        self.values = []
        self.header = True
        self.data = ''


    @staticmethod
    def parse(source, cast, check_errors):
        if not source.strip():
            return ()
        if check_errors:
            errors = re.findall(SqlplusResultParser.REGEXP_ERRORS, source, re.MULTILINE + re.IGNORECASE)
            if errors:
                # raise SqlplusException('\n'.join(errors))
                parsed = SqlplusErrorParser.parse(source)
                raise SqlplusException(parsed)
        # parser = SqlplusResultParser(cast)
        # parser.feed(source)
        # return tuple(parser.result)
        return source

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.active = True
        elif self.active:
            if tag == 'th':
                self.header = True
            elif tag == 'td':
                self.header = False

    def handle_endtag(self, tag):
        if tag == 'table':
            self.active = False
        elif self.active:
            if tag == 'tr' and not self.header:
                row = dict(zip(self.fields, self.values))
                self.result.append(row)
                self.values = []
            elif tag == 'th':
                self.fields.append(self.data.strip())
                self.data = ''
            elif tag == 'td':
                data = self.data.strip()
                if self.cast:
                    data = self._cast(data)
                self.values.append(data)
                self.data = ''

    def handle_data(self, data):
        if self.active:
            self.data += data

    @staticmethod
    def _cast(value):
        for regexp, function in SqlplusResultParser.CASTS:
            if re.match("^%s$" % regexp, value):
                return function(value)
        return value


class SqlplusErrorParser(HTMLParser.HTMLParser):

    NB_ERROR_LINES = 4

    def __init__(self):
        HTMLParser.HTMLParser.__init__(self)
        self.active = False
        self.message = ''

    @staticmethod
    def parse(source):
        parser = SqlplusErrorParser()
        parser.feed(source)
        lines = [l for l in parser.message.split('\n') if l.strip() != '']
        return '\n'.join(lines)

    def handle_starttag(self, tag, attrs):
        if tag == 'body':
            self.active = True

    def handle_endtag(self, tag):
        if tag == 'body':
            self.active = False

    def handle_data(self, data):
        if self.active:
            self.message += data

# pylint: disable=W0231
class SqlplusException(Exception):

    def __init__(self, message, query=None):
        self.message = message
        self.query = query

    def __str__(self):
        return '\n' + self.message
