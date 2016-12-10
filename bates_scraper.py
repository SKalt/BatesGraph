# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 12:27:32 2015

@author: steven
"""

# import block
import os
import re
import string
from lxml import html
import neo4j
import sqlite3
import sqlalchemy
from sqlalchemy import Column, Integer, SmallInteger, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
# class Bates(object):
#     "A class to organize the basic scraping functions"
#     

def get_years():
    """
    Gets the urls of splash pages for all years after 2013-14

    Returns:
        A list of strings in the format '?s=query' indicating catalog years
    """
    page = html.parse('http://www.bates.edu/catalog/?a=catalogList')
    links = page.xpath('//*[@id="catmenu"]//a')
    results = []
    for a in links:
        year = int(a.text.strip()[:4])
        if year > 2012:
            results.append(a.attrib['href'])
    return results

def map_years():
    """
    returns a dict mapping urlencoded year queries to academic year strs.

    Returns:
        as above.
    """
    page = html.parse('http://www.bates.edu/catalog/?s=1000&a=catalogList')
    links = page.xpath('//*[@id="catmenu"]//a')
    string_map = {i.attrib['href']:i.text[:9] for i in links}
    int_map = {}
    for key in string_map:
        int_map[key] = (int(i) for i in string_map[key].split('-'))
    return int_map
    
# main functions in approximate order of their usage
def get_dept_extensions():
    """
    Returns a urlencoded queries specifying departments
    """
    find_links = '//*[@id="deptList"]/div/li/a/@href'
    url = 'http://www.bates.edu/catalog/?s=current'
    tree = html.parse(url)
    return [href.replace('?s=current', '') for href in tree.xpath(find_links)]
    
#%%
def generate_dept_pages():
    """
    Generates a complete list of urls to scrape.

    Returns:
        a list of all department catalog URLs to scrape. Some may not exist,
        since new departments have been added after the minimum year of 
        get_years().
    """
    years = get_years()
    dept_extensions = get_dept_extensions()
    results = []
    for year in years:
        for dept in dept_extensions:
            results.append('http://www.bates.edu/catalog/' + year + dept)
    return results
#%%

#%%
def get_sqlite_db_connection():
    engine = sqlalchemy.create_engine('sqlite+pysqlite:///bates.db')
    return engine
    # cur = conn.cursor()
    # def commit(command):
    #     cur.execute(command)
    #     conn.commit()
    
    # commit(
    #     """
    #     CREATE TABLE IF NOT EXISTS page_status(
    #         url varchar(100),
    #         status char(3),
    #     )
    #     """
    # )
    # commit(
    #     """
    #     CREATE TABLE IF NOT EXISTS courses(
    #         code varchar(10),
    #         description varchar
    #     )
    #     """
    # )
    # commit(
    #     """
    #     CREATE TABLE IF NOT EXISTS professors(
    #     name varchar(50),
    #     code varchar(10),
    #     year integer
    # return conn, cur
def map_codes():
    """
    Returns a dict mapping all department names to department codes and another
    dict mapping all interdisciplinary department shortcodes to department
    codes.

    Returns:
        At tuple of two dicts, DEPT:DT and Dept Name:DEPT.
    """
    root = html.parse('http://www.bates.edu/catalog/').getroot()
    subj_name = root.xpath(".//div[@class='subjName']//li/text()")
    if not subj_name:
        raise ValueError('No results; check xpath')
    subj_name = [i.replace('and', '&') for i in subj_name]
    subj_code = root.xpath(".//div[@class='subjCode']//li/text()")
    subj_code_2 = root.xpath(".//div[@class='subjCodeInt']//li/text()")
    if not len(subj_code) == len(subj_code_2) == len(subj_name):
        raise ValueError('unequal-length code lists')
    else:
        shortcode_map = {}
        names_map = {}
        for i in range(len(subj_name)):
            names_map[subj_name[i]] = subj_code[i]
            shortcode_map[subj_code_2[i]] = subj_code[i]
        return shortcode_map, names_map

class Page(Base):
    """
    Holds functions for scraping a departmental course catalog page.
    """
    
    dept_shortcode_map, dept_name_map = map_codes()
    name_dept_map = {v:k for k, v in dept_name_map}
    year_map = map_years()

    def __init__(self, url, neo4j_session):
        """
        Args:
        url:
        neo4j_session:
        """
        self.url = url
        self.dept_code = url.split('&a=renderDept&d=')[1]
        self.start_year, self.end_year = self.get_years()
        self.page = html.parse(url)
        self.raw_courses = self.page.xpath('//*[@class="Course"]')
        if not self.raw_courses:
            # no courses found, a 404 page
            self.status = 404
            self.dept_name = 'Does not exist at this time'
        else:
            self.status = 200
            self.dept_name = self.name_dept_map[self.dept_code]
            self.courses = []
            for course in self.raw_courses:
                self.courses.append(Course(self, course, neo4j_session))
    # def scrape_page(self, neo4j_session):
    #     """
    #     Scrape all courses on a Bates department course catalog page.

    #     Args:
    #         url: a string url of a Bates departmental catalog page.
    #         session: a neo4j.GraphDatabase.driver.session object.

    #     Returns:
    #         None.
    #     """
    #     courses = self.page.xpath('//*[@class="Course"]')
    #     for course in courses:
    #         scrape_course(course, neo4j_session)
    
    def get_years(self):
        """
        Maps the urlencoded catalog year to a tuple (start_year, end_year)

        Returns:
            a tuple of catalog coverage: (start_year, end_year). The start year
            corresponds to the falls semester year, and the end year corresponds
            to the spring semester year.
        """
        year = self.year_map[self.url.split('?s=')[1].split('&')[0]]
        years = self.years_map[year]
        return years

    url = Column(String(), primary_key=True)
    dept_code = Column(String(4))
    dept_name = Column(String(50))
    start_year = Column(SmallInteger())
    end_year = Column(SmallInteger())
    status = Column(String(10))

class Course(Base):
    "Represents a course description"
    def __init__(self, page_inst, course_div, session):
        """
        Parses html course info into a this Course object.

        Args:
            page_inst: a Page object on which this course is found.
            course_div: a lxml.html._Element representing a div containing
                course details.
            session: a neo4j.GraphDatabase.driver.session object.
        """
        div_id = self.course_div.xpath('./a[2]/@name')[0]
        self.link = page_inst.url + '#' + div_id
        self.start_year = page_inst.start_year
        self.end_year = page_inst.end_year
        # inheret the department code.  Note this will result in multiple
        # listings of crosslisted courses in Bates.db.
        self.dept_code = page_inst.dept_code
        self.name = course_div.xpath('./h4[@class="crsname"]/text()')[0]
        self.code = self.name.split('.')[0]
        self.desc = course_div.xpath('./span[@class="CourseDesc"]/text()')
        self.desc = ' '.join(self.desc)
        self.concentrations = course_div.xpath('./span/div/ul/li/a/text()')
        self.requirements_flag = False
        self.requirements = []

    def parse_reqirements(self):
        """
        Populates self.requirements
        """
        if 'Prerequisite(s):' in self.desc:
            self.requirements_flag = True
            reqs = self.desc.split('Prerequisite(s):')[1].split('.')[0].strip()
            # if '100-level' in reqs:
            #     pass
            # if 'one' in reqs.split():
            #     pass
            current_dept = self.dept_code
            translator = str.maketrans({k:' ' for k in string.punctuation})
            reqs = reqs.translate(translator)
            reqs = (i for i in reqs.split())
            # results = []
            # TODO: figure out how to parse nested 'and' and 'or's
            for chunk in reqs:
                # if chunk.lower() in ['or', 'and']:
                #     results.append(chunk)
                if chunk.isupper() and chunk.isalpha() and len(chunk) > 2:
                    current_dept = chunk
                if chunk[0].isnumeric() and len(chunk) >= 3:
                    Requirement(self, current_dept + ' ' + chunk)

    link = Column(String())
    name = Column(String())
    code = Column(String(10), primary_key = True)
    desc = Column(String())
    requirements_flag = Column(Boolean())
    start_year = Column(SmallInteger())
    end_year = Column(SmallInteger())

class Reqirement(Base):
    requirer = Column(String())
    required = Column(String())
    start_year = Column(SmallInteger())
    end_year = Column(SmallInteger())

    def __init__(self, course_inst, required_course_code):
        self.requirer = course_inst.code
        self.required = required_course_code
        self.start_year = course_inst.start_year
        self.end_year = course_inst.end_year
#%%
if __name__ == "__main__":
#    (COURSES, EL, XLISTINGS) = get_all_available_courses(get_dept_urls())
#    export_all_json(COURSES)
    TEMP = open('Bates.json')
    COURSES = json.loads(TEMP.read())
    TEMP.close()
    EL = get_el(COURSES)
    # TODO: rewrite to overwrite last semester
    G = make_course_graph(COURSES, EL)
    DEPTS = []
    #%%
for code in COURSES:
    DEPTS += COURSES[code]["departments"]
DEPTS = list(set(DEPTS))
for dept in DEPTS:
    print(dept)
    export_json(dept, COURSES, G)
