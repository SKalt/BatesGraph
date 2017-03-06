# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 12:27:32 2015

@author: steven
"""

# import block
import os
import re
import string
from datetime import datetime
import progressbar as pb
from lxml import html
from neo4j.v1 import GraphDatabase, basic_auth

class Bates(object):
    "A class to organize the basic scraping functions"
    def __init__(self):
        """
        Populates object attributes
        """
#        self.splash_pg = html.parse('http://www.bates.edu/catalog/?s=current')
        self.year_link_map = self.map_years()
        self.dept_page_queries = self.get_dept_extensions()
        self.page_query_tuples = self.generate_page_query_tuples()
        maps = self.map_codes()
        self.maps = maps

    def map_years(self, force_page=False):
        """
        returns a dict mapping urlencoded year queries to academic year strs.
        Args:
            force_page: a boolean, whether or not to force refresh the dept
            list from the bates catalog splash page.
        Returns:
            as above.
        """
        def get_page_xml():
            url = 'http://www.bates.edu/catalog/?a=catalogList'
            page = html.parse(url)
            new_catalog_list = page.xpath('//*[@id="catmenu"]')[0]
            with open('./cached_xml/YearMap.xml', 'w') as xml_file:
                xml = html.etree.tostring(new_catalog_list).decode('utf8')
                xml_file.write(xml)
            return new_catalog_list

        if os.path.exists('./cached_xml/YearMap.xml'):
            if force_page:
                catalog_list = get_page_xml()
            else:
                catalog_list = html.parse('./cached_xml/YearMap.xml').getroot()
        else:
            catalog_list = get_page_xml()
        links = catalog_list.xpath('//*[@id="catmenu"]//a')
        string_map = {i.attrib['href']:i.text[:9] for i in links}
        int_map = {}
        for key in string_map:
            int_map[key] = [int(i) for i in re.split(r'\W', string_map[key])]
        if datetime.now().year not in [int_map[k][0] for k in int_map]:
            if not force_page: # in which case the most recent page is cached
                int_map = self.map_years(force_page=True)
        return int_map

    @staticmethod
    def get_dept_extensions(force_page=False):
        """
        Scrapes, returns a list of urlencoded queries specifying departments.
        Args:
            force_page: a boolean whether to force refresh the page
        Returns:
        a list of department page urls.
        """
        def get_page_xml():
            """
            writes the relevant info to an xml file.
            Returns:
            a lxml.html.etree splashpage.
            """
            url = 'http://www.bates.edu/catalog/?s=current'
            page = html.parse(url)
            new_dept_list = page.xpath('//*[@id="deptList"]')[0]
            subj_name = page.xpath('//*[@class="subjName"]')[0]
            subj_code = page.xpath('//*[@class="subjCode"]')[0]
            subj_code_2 = page.xpath('//*[@class="subjCodeInt"]')[0]
            with open('./cached_xml/SplashPage.xml', 'w') as xml_file:
                xml = '<body>'
                xml += html.etree.tostring(new_dept_list).decode('utf8')
                xml += html.etree.tostring(subj_name).decode('utf8')
                xml += html.etree.tostring(subj_code).decode('utf8')
                xml += html.etree.tostring(subj_code_2).decode('utf8')
                xml += '</body>'
                xml_file.write(xml)
            return page

        if os.path.exists('./cached_xml/SplashPage.xml'):
            if force_page:
                dept_list = get_page_xml()
            else:
                dept_list = html.parse('./cached_xml/SplashPage.xml')
        else:
            dept_list = get_page_xml()
        dept_list_xpath = '//*[@id="deptList"]/div/li/a/@href'
        results = []
        for href in dept_list.xpath(dept_list_xpath):
            results.append(href.replace('?s=current', ''))
        return results

    def generate_page_query_tuples(self):
        """
        Returns a list of tuples of dept_query, year_query's
        """
        results = []
        year_queries = [i for i in self.year_link_map]
        year_queries.sort(key=lambda y: self.year_link_map[y][0])
        for year in year_queries:
            if self.year_link_map[year][0] > 2013:
                for dept in self.dept_page_queries:
                    results.append((dept, year))
        return results

    def map_codes(self):
        """
        Returns a dict mapping all department names to department codes and
        another dict mapping all interdisciplinary department shortcodes to
        department codes.

        Returns:
            At tuple of three dicts, DEPT:DT and Dept Name:DEPT.
        """

        if os.path.exists('./cached_xml/SplashPage.xml'):
            page = html.parse('./cached_xml/SplashPage.xml')
        else:
            raise FileNotFoundError('No cached splash page')

        subj_name = page.xpath(".//div[@class='subjName']//li/text()")
        if not subj_name:
            raise ValueError('No results; check xpath')
        subj_name = [i.replace('and', '&') for i in subj_name]
        subj_code = page.xpath(".//div[@class='subjCode']//li/text()")
        subj_code_2 = page.xpath(".//div[@class='subjCodeInt']//li/text()")
        if not len(subj_code) == len(subj_code_2) == len(subj_name):
            raise ValueError('unequal-length code lists')
        else:
            shortcode_dept_map = {}
            names_dept_map = {}
            dept_name_map = {}
            for i, subj in enumerate(subj_name):
                names_dept_map[subj] = subj_code[i]
                shortcode_dept_map[subj_code_2[i]] = subj_code[i]
                dept_name_map[subj_code[i]] = subj
            return shortcode_dept_map, names_dept_map, dept_name_map

class Page(object):
    "Holds the functions to scrape a Bates catalog page"

    def __init__(self, dept_query, year_query):
        """
        Requires a Bates instance BATES.
        """
        if not os.path.exists('cached_xml'):
            os.mkdir('cached_xml')
        self.years = BATES.year_link_map[year_query]
        self.url = 'http://www.bates.edu/catalog/' + year_query + dept_query
        self.dept = dept_query.replace('&a=renderDept&d=', '')
        fname = '{}{}.xml'.format(self.dept, self.years[0])
        if os.path.exists('cached_xml/' + fname):
            print('parsing ' + fname)
            self.page = html.parse('cached_xml/' + fname)
        else:
            print('downloading, parsing ' + fname)
            self.page = html.parse(self.url)
            self.cache(fname)
        self.courses = []

    def cache(self, fname):
        """
        Caches the page.
        """
        xml_file = ''
        try:
            for div in self.page.xpath('//*[@class="Course"]'):
                # each div describes a course; cache only these.
                xml_file += html.etree.tostring(div).decode('utf8')
        except AssertionError:
            pass
        with open('cached_xml/' + fname, 'w') as target:
            target.write(xml_file)

    def parse(self):
        "create Courses out of all course divs within the page"
        try:
            for div in self.page.xpath('//*[@class="Course"]'):
                name = div.xpath('./h4[@class="crsname"]/text()')[0]
                # code = name.split('.')[0]
                div_id = div.xpath('./a[2]/@name')[0]
                desc = '\n'.join(div.xpath('./span[@class="CourseDesc"]/text()'))
                concs = div.xpath('./span/div/ul/li/a/text()') # concentrations
                course = Course(self, name, div_id, desc, concs, SESSION)
                self.courses.append(course)
        except AssertionError:
            pass

class Course(object):
    "Represents a course description"

    def __repr__(self):
        return '<Course {}, {}>'.format(self.code, self.start_year)

    def __init__(self, parent_page, name, div_id, desc, program_tags, session):
        """
        Parses html course info into a this Course object.

        Args:
            parent_page: a Page object on which this course is found.
            name:
            div_id:
            desc:
            concs:
            session:
        """
        self.code, self.title = [i.strip() for i in name.split('.', 1) if i]
        self.title = self.title.replace('"', "'")
        self.link = parent_page.url + '#' + div_id
        self.start_year, self.end_year = parent_page.years
        self.dept = parent_page.dept
        self.desc = desc.replace('"', "'")
        self.program_membership = program_tags
        self.session = session

    def parse_prerequisites(self):
        """
        Populates self.requirements
        """
        if 'Prerequisite(s):' in self.desc:
            requirements = []
            reqs = self.desc.split('Prerequisite(s):')[1].split('.')[0].strip()
            current_dept = self.dept
            # remove all punctuation except /s from the str describing the
            # prerequisites
            not_slash = string.punctuation.replace('/', '')
            translator = str.maketrans({k:' ' for k in not_slash})
            reqs = reqs.translate(translator)
            reqs = (i for i in reqs.split())
            count = 0
            group = {'type':None, 'courses':[]}
            for chunk in reqs:
                if re.match(r'([A-Z]{1,4}|[A-Z]/[A-Z])', chunk):
                    # this is a department code like MATH
                    current_dept = chunk
                elif re.match(r'^[a-z]?[0-9]{2, 3}[a-z]?$', chunk):
                    # this is a course number
                    group['courses'].append(current_dept + ' ' + chunk)
                    if group['type']:
                        # courses in this group are interchangeable prereqs
                        for course in group['courses']:
                            req = Prerequisite(self, course, group['type'])
                            requirements.append(req)
                        group = {'type':None, 'courses':[]}
                        
                elif re.match('(?i)or', chunk):
                    group['type'] = chunk + str(count)
                    count += 1
            for course in group['courses']:
                req = Prerequisite(self, course, group['type'])
                requirements.append(req)
            return requirements

    def parse_professors(self):
        """
        Populates self.professors.
        """
        desc = self.desc.split('Prerequisite(s)')[::-1][0]
        profs = re.findall(r' [A-Z]\. [A-Z]\w+[-]?\w+| staff| Staff', desc)
        if not profs:
            # raise ValueError('Course description does not have teacher listed')
            print('Course description does not have teacher listed')
            return []
        else:
            profs = [i.strip() for i in profs]
            results = []
            for i in profs:
                if i.lower() == 'staff':
                    results.append(Taught(self, 'Staff'))
                else:
                    results.append(Taught(self, i))
            return results

    def parse_program_membership(self):
        "parses both interdisciplinary program and concentration membership"
        programs = []
        for program_str in self.program_membership:
            # program_str is a string.
            code = re.search(r'C\d\d\d', program_str)
            if code:
                programs.append(Concentration(self, program_str, code))
            else:
                programs.append(Program(self, program_str))
        return programs

    def merge(self):
        """
        Merger this code into the connected neo4j database.
        """
        # ensure the course is present, has details
        cypher = """
        MERGE (n:Course {{code:'{code}'}})
        ON CREATE SET
            n.link = "{link}",
            n.title = "{title}",
            n.desc = "{desc}",
            n.requirements_flag = {requirements_flag},
            n.years = [{year}]
        ON MATCH SET
            n.link = "{link}",
            n.title = "{title}",
            n.desc = "{desc}",
            n.requirements_flag = {requirements_flag}
        """.format(
            code=self.code,
            link=self.link,
            title=self.title,
            desc=self.desc,
            requirements_flag=('Prerequisite(s):' in self.desc),
            year=self.start_year
        )
        self.session.run(cypher)
        cypher = """
        MATCH (n:Course {{code:'{code}'}})
        UNWIND n.years + {year} AS years
        WITH n, COLLECT(DISTINCT years) as unique_years
            SET n.years = unique_years
        """.format(
            code=self.code,
            year=self.start_year
        ) # verified
        self.session.run(cypher)
        return

    def merge_profs(self):
        """
        Merge any connected professors into the connected neo4j db.
        """
        profs = self.parse_professors()
        for prof in profs:
            prof.merge()
            
    def merge_prereqs(self):
        """
        Merge any of this course's requirements into the connected neo4j db.
        """
        prereqs = self.parse_prerequisites()
        if prereqs:
            for prereq in prereqs:
                prereq.merge()
            
    def merge_program_membership(self):
        """
        Merge this course's membership relations into the connected neo4j db.
        """
        programs = self.parse_program_membership()
        programs.append(Dept(self))
        for program in programs:
            program.merge()

class Dept(object):
    "Represents a link of a course belonging to a department"
    def __init__(self, course_inst):
        self.session = course_inst.session
        self.member = course_inst.code
        self.dept_code = course_inst.dept
        self.year = course_inst.start_year

    def __repr__(self):
        return '<{} {}>'.format(self.dept_code, self.year)

    def merge(self):
        """
        Merges this department-course relation into the connected neo4j db.
        """
        # ensure the dept exists, and Dept, In_Dept have unique year arrays with
        # the current year
        # TODO : change In_Program, In_Dept to In
        cypher = """
        MERGE (d:Dept {{code:'{code}'}})
        ON CREATE SET d.years = [{year}]
        MERGE (c:Course {{code:'{course_code}'}})
        MERGE (c)-[i:In_Dept]->(d)
        ON CREATE SET i.years = [{year}]
        WITH d, i
            UNWIND d.years + {year} as d_years
            UNWIND i.years + {year} AS i_years
        WITH d, i, 
            COLLECT(DISTINCT d_years) AS d_unique_years,
            COLLECT(DISTINCT i_years) AS i_unique_years
                SET d.years = d_unique_years
                SET i.years = i_unique_years
        RETURN d.years, i.years
            """.format(
                year=self.year,
                code=self.dept_code,
                course_code=self.member
            )
        result = self.session.run(cypher)
        return result

class Prerequisite(object):
    "Represents a course-requires-course relationship"
    def __init__(self, course_inst, required_course_code, label):
        self.requirer = course_inst.code
        self.required = required_course_code
        self.year = course_inst.start_year
        self.label = label
        self.session = course_inst.session

    def merge(self):
        """
        Merges this course-requires-course relationship into the connected
        neo4j db.
        """
        # ensure prereq node, link exists, and link.year is a non-empty array
        cypher = """
        MERGE (n:Course {{code:'{n}'}})
        MERGE (m:Course {{code:'{m}'}})
        MERGE (n)-[r:Prereq_To]->(m)
            ON CREATE SET r.years = [{year}], r.label = '{label}'
            ON MATCH SET r.label = '{label}'
        WITH r
            UNWIND r.years + {year} AS req_years
        WITH r, COLLECT(DISTINCT req_years) as unique_years
            SET r.years = unique_years
        """.format(
            m=self.requirer,
            n=self.required,
            year=self.year,
            label=self.label
        ) # verified
        self.session.run(cypher)
        # # ensure Prereq_To.years members are unique and contain self.year
        # cypher = """
        # MATCH (n:Course {{code:'{n}'}})-[r:Prereq_To]->(m:Course {{code:'{m}'}})
        # UNWIND r.years + {year} AS req_years
        # WITH r, COLLECT(DISTINCT req_years) as unique_years
        # SET r.years = unique_years
        # """.format(
        #     m=self.requirer,
        #     n=self.required,
        #     year=self.year
        # ) # verified
        # self.session.run(cypher)
        #TODO: consolidate program, concentration, dept cql commands to one formatted string
    def __repr__(self):
        return '<Prereq {}->{} {}>'.format(
            self.required,
            self.requirer,
            self.year
        )

class Program(object):
    "Represents a course-in-program relationship."
    def __init__(self, course_inst, program_name):
        self.year = course_inst.start_year
        self.course_code = course_inst.code
        self.name = program_name.split('(')[0].strip()
        self.session = course_inst.session
        self.label = 'Program'

    def __repr__(self):
        return '<{} {} {}>'.format(
            self.label,
            self.name,
            self.year
            )

    def merge(self):
        "Merges this course-in-program relationship into the connected neo4j db"
        # ensure Program node, In_Program in db, and have unique .year arrays
        cypher = """
        MERGE (p:Program {{name:'{name}'}})
            ON CREATE SET p.years = [{year}]
        MERGE (c:Course {{code:'{code}'}})
        MERGE (p)<-[i:In_Program]-(c)
            ON CREATE SET i.years = [{year}]
        WITH p, i
            UNWIND p.years + {year} as p_years
            UNWIND i.years + {year} as i_years
        WITH p, i,
            COLLECT(DISTINCT p_years) AS p_unique_years,
            COLLECT(DISTINCT i_years) AS i_unique_years
                SET p.years = p_unique_years
                SET i.years = i_unique_years
        """.format(
            code=self.course_code,
            year=self.year,
            name=self.name
        ) # verified effectiveness in neo4j console
        # print(cypher)
        results = [i for i in self.session.run(cypher)]
        # ensure Program.years, In_Program.years unique and contain self.year
        # cypher = """
        # MATCH (p:Program {{name:'{name}'}})
        # UNWIND p.years + {year} AS program_years
        # WITH p, COLLECT(DISTINCT program_years) AS unique_years
        #     SET p.years = unique_years

        # WITH p
        #     MATCH (p)<-[i:In_Program]-(c:Course {{code:'{code}'}})
        #     UNWIND i.years + {year} AS membership_years
        #     WITH i, COLLECT(DISTINCT membership_years) AS unique_membership_years
        #         SET i.years = unique_membership_years
        # """.format(
        #     code=self.course_code,
        #     name=self.name,
        #     year=self.year,
        # ) # verified effective despite the odd with clause
        # print(cypher)
        # print(self.session.run(cypher))
        return

class Concentration(Program):
    "Represents a course-in-program relationship"
    def __init__(self, course_inst, conc_name, conc_code):
        Program.__init__(self, course_inst, conc_name)
        self.code = conc_code
        self.label = 'Concentration'
    def merge(self):
        "Merges the course-in-program relationship into the connected neo4j db."
        # ensure Concentration, In_Program exist with .year arrays
        cypher = """
        MERGE (con:Concentration {{name:'{name}'}})
            ON CREATE SET con.years = [{year}]
        MERGE (cou:Course {{code:'{code}'}})
        MERGE (con)<-[i:In_Program]-(cou)
            ON CREATE SET i.years = [{year}]
        WITH con, i
            UNWIND con.years + {year} as con_years
            UNWIND i.years + {year} as i_years
        WITH con, i,
            COLLECT(DISTINCT con_years) AS con_unique_years,
            COLLECT(DISTINCT i_years) AS i_unique_years
                SET con.years = con_unique_years
                SET i.years = i_unique_years
        RETURN con.years, i.years
        """.format(
            code=self.course_code,
            year=self.year,
            name=self.name
        )
        # print(cypher)
        results = self.session.run(cypher)
        return results

class Taught(object):
    "Represents a teacher-taught-course relationship"
    def __init__(self, course_inst, prof_name):
        self.year = course_inst.start_year
        self.prof_name = prof_name
        self.course = course_inst.code
        self.session = course_inst.session

    def __repr__(self):
        return '<Taught {}->{} {}>'.format(
            self.prof_name,
            self.course,
            self.year
            )
    
    def merge(self):
        "Merges this teacher-course relationship into the connected neo4j db."
        # ensure prof, taught link exist & have unique, current .year arrays
        cypher = """
        MERGE (p:Prof {{name:'{name}'}})
            ON CREATE SET p.years = [{year}]
        MERGE (c:Course {{code:'{code}'}})
        MERGE (p)-[t:Taught]->(c)
                ON CREATE SET t.years = [{year}]
        WITH p, t
            UNWIND t.years + {year} AS taught_years
            UNWIND p.years + {year} AS prof_years
        WITH p, t,
            COLLECT(DISTINCT taught_years) AS t_unique_years,
            COLLECT(DISTINCT prof_years) AS p_unique_years
                SET t.years = t_unique_years
                SET p.years = p_unique_years 
        """.format(
            code=self.course,
            name=self.prof_name,
            year=self.year
        )
        records = [i for i in self.session.run(cypher)]
        return records

#%%
get_xml_year = lambda(xml_filename): int(xml_filename[::-1][4:8][::-1])

if __name__ == "__main__":
    if os.getcwd().split('/')[::-1][0] != 'BatesGraph':
        os.chdir('BatesGraph')
        # will raise an informative error if not in the correct directory
    if 'USR' not in dir():
        USR = input('username: ')
        PWD = input('password: ')
    DRIVER = GraphDatabase.driver(
        "bolt://localhost",
        auth=basic_auth(USR, PWD)
    )
    SESSION = DRIVER.session()
    SESSION.run('CREATE CONSTRAINT ON (c:Course) ASSERT c.code IS UNIQUE')
    SESSION.run('CREATE CONSTRAINT ON (d:Dept) ASSERT d.code IS UNIQUE')
    SESSION.run('CREATE CONSTRAINT ON (p:Prof) ASSERT p.name IS UNIQUE')
    SESSION.run('CREATE CONSTRAINT ON (p:Program) ASSERT p.code IS UNIQUE')
    BATES = Bates()
    for dept, year in BATES.page_query_tuples:
        PAGE = Page(dept, year)
        PAGE.parse()
        for course in PAGE.courses:
            course.merge()
            course.merge_profs()
            course.merge_prereqs()
            course.merge_program_membership()

    CACHED_PAGES = os.listdir('./cached_xml')
    CACHED_PAGES = [i for i in CACHED_PAGES if i[::-1][4:8].isnumeric()]
    CACHED_PAGES.sort(key=lambda fname: int(fname[::-1][4:8][::-1]))
    # sorted by year in filename
    PROG = pb.ProgressBar()
    for dept, year in PROG(BATES.page_query_tuples):
        PAGE = Page(dept, year)
#%% old tests
# p = Page('&a=renderDept&d=WGST', '?s=1097')
# p.parse()
# c = p.courses[0]
# print(c)
# print(c.parse_professors())
# #%%
# c.merge()
# #%%
# p = c.parse_professors()[0]
# p.merge()
# #%%
# c.merge_profs()
# #%%
# programs = c.parse_program_membership()
# p = programs[0]
# #c.merge_program_membership()
# #%%
# print(c.parse_requirements())
# #%%
# PROG = pb.ProgressBar()
# for dept, year in PROG(BATES.page_query_tuples):
#     PAGE = Page(dept, year)
#     #%%
