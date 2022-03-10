from fullcontact import FullContactClient

if __name__ == '__main__':
    api_key = 'mRuFj8wJBXK7g4uGgARkNA99dqd1BLi8'
    client = FullContactClient(api_key)
    ret = client.person.enrich(email='jgeskey@verizon.net')
    print(ret)
    if ret.is_successful:
        print(ret.response.json())
