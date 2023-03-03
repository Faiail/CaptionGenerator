from neo4j import GraphDatabase
import pandas as pd


class DataManager:
    """
    Class that performs query to the Graph Database
    """
    def __init__(self, driver: GraphDatabase = None):
        """
        :param driver: default db connector
        """
        try:
            self.driver = driver if driver else GraphDatabase.driver(uri="bolt://localhost:7687", auth=('neo4j', 'neo4j'))
        except:
            print("Warning: cannot connect to DB. Only local operations can be performed.")

    def get_artworks(self, database: str = 'neo4j') -> list[str]:
        """

        :param database: string value that indicates the target database within the connection
        :return: A list of strings. Each of these strings is an artwork name
        """
        with self.driver.session(database=database) as session:
            query = 'match (a:Artwork) return a.name as name'
            ans = list(map(lambda x: x['name'], session.run(query)))
        return ans

    def get_artwork_title(self, name: str, database: str = 'neo4j') -> str:
        """

        :param name: a string value that indicates file name of the artwork
        :param database: string value that indicates the target database within the connection
        :return: a string, indicating the title of the artwork
        """
        with self.driver.session(database=database) as session:
            query = f'match (a:Artwork{name}) return a.title as title'
            ans = list(map(lambda x: x['title'], session.run(query)))
        return ans[0]

    def __get_artwork_date(self, name: str, database: str = 'neo4j') -> str:
        """

        :param name: a string value that indicates file name of the artwork
        :param database: string value that indicates the target database within the connection
        :return: the date indicating in which period the target artwork has been made
        """
        with self.driver.session(database=database) as session:
            query = f'match (a:Artwork{name}) return a.date as date'
            ans = list(map(lambda x: x['date'], session.run(query)))
        return ans[0] if ans[0] else ""

    def get_neighbor_types(self, database: str = 'neo4j', node_type: str = 'Artwork') -> list[str]:
        """

        :param database: string value that indicates the target database within the connection
        :param node_type: The label of the node
        :return: A list of strings, indicating all the types of neighbors for the given node
        """
        with self.driver.session(database=database) as session:
            query = f'match (a:{node_type})--(n) return distinct labels(n)[0] as types'
            ans = list(map(lambda x: x['types'], session.run(query)))
        return list(filter(lambda x: x not in ["Emotion", "Period"], ans))

    def get_attributes(self, head_type: str, head: str, database: str = 'neo4j') -> list:
        """

        :param head_type: string value, indicting the subject node type
        :param head: the subject node instance
        :param database: string value that indicates the target database within the connection
        :return: all the attributes for the given head node
        """
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

    def get_template(self, head_type: str, head_name: str, database: str = 'neo4j') -> list:
        """

        :param head_type: string value, indicting the subject node type
        :param head_name: the subject node instance
        :param database: string value that indicates the target database within the connection
        :return: A list of attributes type and values for the given head node
        """
        a = self.get_attributes(head_type, head_name, database)
        return a + [['Date', self.__get_artwork_date(head_name, database)], ['Title', self.get_artwork_title(head_name, database)]]

    def get_prompt(self, template: list) -> str:
        """

        :param template: A list indicating all information in a structured way
        :return: A prompt reflecting the same information stored in the template
        """
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
        """

        :param name: The name of the target artwork
        :param database: string value that indicates the target database within the connection
        :return:
        """
        return {k: v for k, v in filter(lambda x: x[1] != '', self.get_template("Artwork", f'{{name: "{name}"}}', database))}

    def get_prompt_by_artwork(self, name: str):
        """

        :param name: the name of the target artwork
        :return: A prompt of the target artwork
        """
        template = {k: v for k, v in filter(lambda x: x[1] != '', self.get_template("Artwork", f'{{name: "{name}"}}'))}
        return self.get_prompt(template)


if __name__ == '__main__':
    # test with a random artwork
    import random
    manager = DataManager()
    artwork = random.choice(manager.get_artworks())
    print(manager.get_prompt_by_artwork(artwork))
