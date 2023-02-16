from neo4j import GraphDatabase
import pandas as pd


class DataManager():
    def __init__(self, driver: GraphDatabase = None):
        self.driver = driver if driver else GraphDatabase.driver(uri="bolt://localhost:7687", auth=('neo4j', 'neo4j'))

    def get_artworks(self, database: str = 'neo4j'):
        with self.driver.session(database=database) as session:
            query = 'match (a:Artwork) return a.name as name'
            ans = list(map(lambda x: x['name'], session.run(query)))
        return ans

    def get_artwork_title(self, name: str, database: str = 'neo4j'):
        with self.driver.session(database=database) as session:
            query = f'match (a:Artwork{name}) return a.title as title'
            ans = list(map(lambda x: x['title'], session.run(query)))
        return ans[0]

    def __get_artwork_date(self, name: str, database: str = 'neo4j'):
        with self.driver.session(database=database) as session:
            query = f'match (a:Artwork{name}) return a.date as date'
            ans = list(map(lambda x: x['date'], session.run(query)))
        return ans[0] if ans[0] else ""

    def get_neighbor_types(self, database: str = 'neo4j', node_type: str = 'Artwork'):
        with self.driver.session(database=database) as session:
            query = f'match (a:{node_type})--(n) return distinct labels(n)[0] as types'
            ans = list(map(lambda x: x['types'], session.run(query)))
        return list(filter(lambda x: x not in ["Emotion", "Period"], ans))

    def __get_city(self, head: str, city: str, database: str = 'neo4j'):
        with self.driver.session(database=database) as session:
            query = f'match (h:Artwork{head})--()--(t:{city}) return t.name as tail'
            ans = list(map(lambda x: x['tail'], session.run(query)))
        if len(ans) == 0:
            return ""
        return ans[0]

    def get_attributes(self, head_type, head, database: str = 'neo4j'):
        with self.driver.session(database=database) as session:
            query = f"""match (h:{head_type}{head})--(t) where not labels(t)[0] in ["Emotion", "Period"]
            return t.name as name, t.printed_name as printed_name, labels(t)[0] as type"""
            ans = list(map(tuple, session.run(query)))
        df = pd.DataFrame(ans, columns=['name', 'printed_name', 'type'])
        df.name = df.apply(lambda x: x[1] if x[2] == 'Artist' else x[0], axis=1)
        df.drop('printed_name', axis=1, inplace=True)
        df = df.groupby(by='type')['name'].apply(list).reset_index(name='names')

        df['names'] = df.apply(lambda x: f'{", ".join(x[1][:-1])} and {x[1][-1]}'.strip() if len(x[1]) > 1 \
                               else f'{x[1][0]}'.strip(), axis=1)
        return df.values.tolist()

    def get_attribute(self, head_type: str, head: str, tail: str, database: str = 'neo4j'):
        if tail.lower() == 'city':
            return self.__get_city(head, tail, database)
        with self.driver.session(database=database) as session:
            query = f'match (h:{head_type}{head})--(t:{tail}) return t.{"printed_" if tail == "Artist" else ""}name as tail'
            ans = list(map(lambda x: x['tail'].replace('-', ' '), session.run(query)))
        if len(ans) > 1:
            return f'{", ".join(ans[:-1])} and {ans[-1]}'.strip()
        if len(ans) == 0:
            return ""
        return f'{ans[0]}'.strip()

    def get_template(self, head_type: str, head_name: str, database: str = 'neo4j'):
        types = self.get_neighbor_types(database, head_type)
        a = self.get_attributes(head_type, head_name, database)
        #a = list(zip(types, list(map(lambda x: self.get_attribute(head_type, head_name, x, database), types))))
        return a + [['Date', self.__get_artwork_date(head_name, database)], ['Title', self.get_artwork_title(head_name, database)]]

    def get_prompt(self, template):
        base = f""""{template['Title']}", painted by {template['Artist']}"""
        if 'Media' in template:
            base += f" on {template['Media']}"
        if 'Tag' in template:
            base += f" regarding {template['Tag']}"
        base += '.\n'
        if 'Date' in template:
            base += f"It has been done in {template['Date']}.\n"
        base += f"It is a {template['Genre']} in the style of {template['Style']}"
        if 'Serie' in template:
            base += f", belonging to the serie \"{template['Serie']}\""
        base += '.'
        if not ('Gallery' in template or 'City' in template or 'Country' in template):
            return base
        base += '\nThe painting is hosted in'
        if 'Gallery' in template:
            base += f" {template['Gallery']}"
        if 'City' in template:
            base += f", {template['City']}"
        if 'Country' in template:
            base += f", {template['Country']}"
        base += '.'
        return base

    def get_template_by_artwork(self, name: str, database: str = 'neo4j'):
        return {k: v for k, v in filter(lambda x: x[1] != '', self.get_template("Artwork", f'{{name: "{name}"}}', database))}

    def get_prompt_by_artwork(self, name: str):
        template = {k: v for k, v in filter(lambda x: x[1] != '', self.get_template("Artwork", f'{{name: "{name}"}}'))}
        return self.get_prompt(template)


if __name__ == '__main__':
    import random
    manager = DataManager()
    artwork = random.choice(manager.get_artworks())
    print(manager.get_prompt_by_artwork(artwork))


    #print(artwork)
    #print(manager.get_attributes("Artwork", artwork))
