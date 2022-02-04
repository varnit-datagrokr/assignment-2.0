from asyncio.proactor_events import _ProactorBaseWritePipeTransport
import csv
import pandas as pd
import pycountry_convert as pc
import json

class SurveyResult:
    def __init__(self) -> None:
        # self.df = pd.read_csv('survey_results_public.csv')
        # print(self.df)
        data = []
        with open('etl_database/survey_results_public.csv','r') as rf:
            reader = csv.reader(rf)
            for i in reader:
                data.append(i)
        self.data = data

    def _return_index(self,column_name:str) -> int:
        for row in self.data:
            for i in range(len(row)):
                if row[i] == column_name:
                    return i
                    
            break
        return -1

    def average_age_of_devs(self) -> int:
        '''Returns the average age of developers when they wrote their first line of code.'''
        average_age = 0
        count = 0
        for row in self.data[1:]:
            if row[14] == 'Older than 85':
                average_age += 85
            elif row[14] == 'Younger than 5 years':
                average_age += 5
            elif row[14] == 'NA':
                continue
            else:
                average_age += int(row[14])

            count += 1

        return round(average_age/count,2)

    def know_python(self) -> list:
        '''Percentage of developers who knew python in each country.'''
        language_index = self._return_index('LanguageWorkedWith')
        country_index = self._return_index('Country')
        developers = {}
        for row in self.data[1:]:
            know_python = 0
            if row[language_index].find('Python') != -1:
                know_python = 1
            if row[country_index] in developers:
                developers[row[country_index]]['know_python'] += know_python
                developers[row[country_index]]['total'] += 1
            else:
                
                developers[row[country_index]] = {
                    "know_python": know_python,
                    "total": 1
                }
            
        result = []        
        for country in developers.keys():
            result.append([country,round((developers[country]['know_python']/developers[country]['total'])*100,2)])
        # for i in result:
        #     print(i)

        return result

    def average_salary(self) -> list:
        '''Returns average salary of developers based on the continent.'''
        country_index = self._return_index('Country')
        salary_index = self._return_index('ConvertedComp')
        continent = {}
        for row in self.data[1:]:
            try:
                country_code = pc.country_name_to_country_alpha2(row[country_index], cn_name_format="default")
                continent_name = pc.country_alpha2_to_continent_code(country_code)
                if continent_name in continent:
                    if row[salary_index] != 'NA':
                        continent[continent_name]["salary"] += float(row[salary_index])
                        continent[continent_name]["total"] += 1
                else:
                    if row[salary_index] != 'NA':
                        continent[continent_name] = {
                            "salary": float(row[salary_index]),
                            "total": 1
                        }
            except Exception as e:
                # print(row[country_index])
                pass
                

        result_list = []
        for con in continent.keys():
            continents_full_name = {
                'NA': 'North America',
                'SA': 'South America', 
                'AS': 'Asia',
                'OC': 'Australia',
                'AF': 'Africa',
                'EU': 'Europe'
            }
            result_list.append([continents_full_name[con],round(continent[con]['salary']/continent[con]['total'],2)])

        # for i in result_list:
        #     print(i)

        return result_list

    def most_desired_language(self) -> str: 
        '''Returns the most desired programming language for the year  2020.'''
        desired_lng_index = self._return_index('LanguageDesireNextYear')
        lngs = {}
        for row in self.data[1:]:
            languages = row[desired_lng_index].split(";")
            for lng in languages:
                if lng in lngs:
                    lngs[lng] += 1
                else:
                    lngs[lng] = 1

        lngs = {k: v for k, v in sorted(lngs.items(), key=lambda item: item[1],reverse=True)}
        return list(lngs)[0]

    def hobby_devs(self) -> list:
        '''Returns the report for the people who code as a hobby based on their gender and continent.'''
        country_index = self._return_index('Country')
        hobby_index = self._return_index('Hobbyist')
        gender_index = self._return_index('Gender')
        continent = {}
        for row in self.data[1:]:
            try:
                if row[hobby_index] == 'No':
                    continue
                country_code = pc.country_name_to_country_alpha2(row[country_index], cn_name_format="default")
                continent_name = pc.country_alpha2_to_continent_code(country_code)
                if continent_name in continent:
                    pass
                else:
                    continent[continent_name] = {
                        "male": 0,
                        "female": 0,
                        "other": 0
                        }
                    
                if row[gender_index] == 'Man':
                    continent[continent_name]['male'] += 1
                elif row[gender_index] == 'Woman':
                    continent[continent_name]['female'] += 1
                else:
                    continent[continent_name]['other'] += 1
            
            except Exception as e:
                # print(row[country_index])
                pass

        result_list = []
        for con in continent.keys():
            continents_full_name = {
                'NA': 'North America',
                'SA': 'South America', 
                'AS': 'Asia',
                'OC': 'Australia',
                'AF': 'Africa',
                'EU': 'Europe'
            }
            male = continent[con]['male']
            female = continent[con]['female']
            other = continent[con]['other']
            total = male + female + other
            result_list.append([continents_full_name[con],round(male*100/total,2),round(female*100/total,2),round(other*100/total,2)])

        # for i in result_list:
        #     print(i)

        return result_list
            
    def satisfaction_devs(self):
        country_index = self._return_index('Country')
        career_sat_index = self._return_index('CareerSat')
        gender_index = self._return_index('Gender')
        continent = {}
        rating = {
            "Very dissatisfied": 0,
            "Slightly dissatisfied": 0,
            "Neither satisfied nor dissatisfied": 0,
            "Slightly satisfied": 0,
            "Very satisfied": 0
        }
        # satisfaction = {}
        for row in self.data[1:]:
        #     if row[career_sat_index] in satisfaction:
        #         pass
        #     else:
        #         satisfaction[row[career_sat_index]] = 0

            try:
                country_code = pc.country_name_to_country_alpha2(row[country_index], cn_name_format="default")
                continent_name = pc.country_alpha2_to_continent_code(country_code)
                
                if continent_name in continent:
                    pass
                else:
                    continent[continent_name] = {
                        "male": {
                            "count": 0,
                            "rating": dict(rating)
                        },
                        "female": {
                            "count": 0,
                            "rating": dict(rating)
                        },
                        "other": {
                            "count": 0,
                            "rating": dict(rating)
                        },
                        }

                if row[career_sat_index] == "NA":
                    continue
                    
                if row[gender_index] == 'Man':
                    continent[continent_name]['male']["count"] += 1
                    continent[continent_name]['male']["rating"][row[career_sat_index]] += 1
                elif row[gender_index] == 'Woman':
                    continent[continent_name]['female']["count"] += 1
                    continent[continent_name]['female']["rating"][row[career_sat_index]] += 1
                else:
                    continent[continent_name]['other']["count"] += 1
                    continent[continent_name]['other']["rating"][row[career_sat_index]] += 1
            
            except Exception as e:
                # print(row[country_index])
                pass

        result_dict = {}
        for con in continent.keys():
            continents_full_name = {
                'NA': 'North America',
                'SA': 'South America', 
                'AS': 'Asia',
                'OC': 'Australia',
                'AF': 'Africa',
                'EU': 'Europe'
            }
            male = continent[con]['male']["count"]
            female = continent[con]['female']["count"]
            other = continent[con]['other']["count"]
            total = male + female + other
            result_dict[continents_full_name[con]] = {
                "male": {
                    "percentage": round(male*100/total,2),
                    "rating": self._get_rating(continent[con]["male"]["rating"])
                },
                "female": {
                    "percentage": round(female*100/total,2),
                    "rating": self._get_rating(continent[con]["female"]["rating"])
                },
                "other": {
                    "percentage": round(other*100/total,2),
                    "rating": self._get_rating(continent[con]["other"]["rating"])
                },
            }

        return json.dumps(result_dict,indent=4)

    def _get_rating(self,rating:dict):
        total = 0
        for key in rating.keys():
            total += rating[key]

        for key in rating.keys():
            rating[key] = round(rating[key]*100/total,2)

        return rating


if __name__ == '__main__':
    SR = SurveyResult()
    print(SR.average_age_of_devs())
    print(SR.know_python())
    print(SR.average_salary())
    print(SR.most_desired_language())
    print(SR.hobby_devs())
    print(SR.satisfaction_devs())
    