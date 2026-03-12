import importlib.util
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "springfield_price_pipeline.py"
FIXTURE = ROOT / "tests" / "fixtures" / "bedworld_sample.html"

spec = importlib.util.spec_from_file_location("springfield_price_pipeline", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)


class SpringfieldPricePipelineTest(unittest.TestCase):
    def setUp(self):
        self._pricesapi_key = module.os.environ.pop("SPRINGFIELD_PRICE_PRICESAPI_KEY", None)
        self._brightdata_api_key = module.os.environ.pop("SPRINGFIELD_PRICE_BRIGHTDATA_API_KEY", None)
        self._brightdata_zone = module.os.environ.pop("SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ZONE", None)
        self._cache_enabled = module.os.environ.get("SPRINGFIELD_PRICE_CACHE_ENABLED")
        self._cache_db_path = module.os.environ.get("SPRINGFIELD_PRICE_CACHE_DB_PATH")
        self._cache_dir = TemporaryDirectory()
        module.os.environ["SPRINGFIELD_PRICE_CACHE_ENABLED"] = "1"
        module.os.environ["SPRINGFIELD_PRICE_CACHE_DB_PATH"] = str(Path(self._cache_dir.name) / "search_cache.sqlite3")

    def tearDown(self):
        if self._pricesapi_key is None:
            module.os.environ.pop("SPRINGFIELD_PRICE_PRICESAPI_KEY", None)
        else:
            module.os.environ["SPRINGFIELD_PRICE_PRICESAPI_KEY"] = self._pricesapi_key
        if self._brightdata_api_key is None:
            module.os.environ.pop("SPRINGFIELD_PRICE_BRIGHTDATA_API_KEY", None)
        else:
            module.os.environ["SPRINGFIELD_PRICE_BRIGHTDATA_API_KEY"] = self._brightdata_api_key
        if self._brightdata_zone is None:
            module.os.environ.pop("SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ZONE", None)
        else:
            module.os.environ["SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ZONE"] = self._brightdata_zone
        if self._cache_enabled is None:
            module.os.environ.pop("SPRINGFIELD_PRICE_CACHE_ENABLED", None)
        else:
            module.os.environ["SPRINGFIELD_PRICE_CACHE_ENABLED"] = self._cache_enabled
        if self._cache_db_path is None:
            module.os.environ.pop("SPRINGFIELD_PRICE_CACHE_DB_PATH", None)
        else:
            module.os.environ["SPRINGFIELD_PRICE_CACHE_DB_PATH"] = self._cache_db_path
        self._cache_dir.cleanup()

    def test_retailer_query_prefers_community_dataset(self):
        old_path = module.COMMUNITY_DATASET_CSV_PATH
        module.COMMUNITY_DATASET_CSV_PATH = ROOT / "tests" / "fixtures" / "missing-community.csv"
        try:
            result = module.analyze_payload("Where can I compare the best value eggs across Tesco, Asda and Sainsburys this week?")
        finally:
            module.COMMUNITY_DATASET_CSV_PATH = old_path
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.query_type, "retailer_comparison")
        self.assertEqual(result.source, "Community UK supermarket time-series dataset")
        self.assertIn("Retailers mentioned: Tesco, ASDA, Sainsbury's", result.reply_message)
        self.assertIn("Community UK supermarket time-series dataset", result.reply_message)
        self.assertIn("Tip: Send a public supermarket product URL", result.reply_message)

    def test_direct_item_lookup_uses_offer_matches_when_available(self):
        old = module.find_direct_item_offers
        module.find_direct_item_offers = lambda plan: [
            {
                "retailer": "Tesco",
                "product_name": "Tesco Finest Wagyu Beef Burger",
                "price_gbp": 6.5,
                "price_unit_gbp": 13.0,
                "unit": "kg",
                "capture_date": "2024-04-10T00:00:00",
                "is_own_brand": True,
                "score": 2,
            },
            {
                "retailer": "ASDA",
                "product_name": "ASDA Wagyu Beef Burger",
                "price_gbp": 7.0,
                "price_unit_gbp": 14.0,
                "unit": "kg",
                "capture_date": "2024-04-09T00:00:00",
                "score": 2,
            },
        ]
        try:
            result = module.analyze_payload("Where to buy UK wagyu beef with best price?")
        finally:
            module.find_direct_item_offers = old
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.source, "Community UK supermarket time-series dataset")
        self.assertIn("Best retailer matches from community dataset", result.reply_message)
        self.assertIn("Comparison basis: standardized £/kg, then shelf price", result.reply_message)
        self.assertIn("Tesco Finest Wagyu Beef Burger - £6.50 (£13.00/kg) - own brand", result.reply_message)
        self.assertIn("ASDA Wagyu Beef Burger - £7.00 (£14.00/kg)", result.reply_message)

    def test_fetch_url_uses_sqlite_cache(self):
        old_get = module.requests.get
        calls = []

        class FakeResponse:
            def __init__(self):
                self.text = "<html><title>Cached Product</title><meta property='product:price:amount' content='4.20'></html>"
                self.url = "https://example.com/product"
                self.headers = {"content-type": "text/html; charset=utf-8"}

            def raise_for_status(self):
                return None

        def fake_get(url, headers=None, timeout=None):
            calls.append(url)
            return FakeResponse()

        module.requests.get = fake_get
        try:
            first = module.fetch_url("https://example.com/product")
            second = module.fetch_url("https://example.com/product")
        finally:
            module.requests.get = old_get
        self.assertEqual(calls, ["https://example.com/product"])
        self.assertEqual(first, second)
        self.assertTrue(Path(module.os.environ["SPRINGFIELD_PRICE_CACHE_DB_PATH"]).exists())

    def test_pricesapi_search_uses_sqlite_cache(self):
        old_get = module.requests.get
        calls = []
        module.os.environ["SPRINGFIELD_PRICE_PRICESAPI_KEY"] = "pricesapi-test-key"

        class FakeResponse:
            def __init__(self):
                self.headers = {"content-type": "application/json"}

            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "data": {
                        "results": [
                            {"id": "wagyu-1", "title": "Wagyu Beef Burgers", "offerCount": 2},
                        ]
                    }
                }

        def fake_get(url, headers=None, params=None, timeout=None):
            calls.append((url, tuple(sorted((params or {}).items()))))
            return FakeResponse()

        module.requests.get = fake_get
        try:
            first = module.fetch_pricesapi_search_results("wagyu beef")
            second = module.fetch_pricesapi_search_results("wagyu beef")
        finally:
            module.requests.get = old_get
        self.assertEqual(
            calls,
            [(f"{module.PRICESAPI_BASE_URL}/products/search", (("limit", module.PRICESAPI_SEARCH_LIMIT), ("q", "wagyu beef")))],
        )
        self.assertEqual(first, second)
        self.assertEqual(first[0]["id"], "wagyu-1")

    def test_brightdata_lookup_uses_sqlite_cache(self):
        old_post = module.requests.post
        calls = []
        module.os.environ["SPRINGFIELD_PRICE_BRIGHTDATA_API_KEY"] = "test-key"
        module.os.environ["SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ZONE"] = "test-zone"

        class FakeResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "shopping": [
                        {
                            "title": "Tesco Wagyu Burgers",
                            "price": "£5.00",
                            "shop": "Tesco",
                            "link": "https://www.tesco.com/groceries/en-GB/products/123",
                        }
                    ]
                }

        def fake_post(url, headers=None, json=None, timeout=None):
            calls.append((url, json["url"], json["zone"]))
            return FakeResponse()

        module.requests.post = fake_post
        try:
            first = module.fetch_brightdata_shopping_results("wagyu beef Tesco")
            second = module.fetch_brightdata_shopping_results("wagyu beef Tesco")
        finally:
            module.requests.post = old_post
        self.assertEqual(
            calls,
            [(
                module.BRIGHTDATA_SERP_ENDPOINT,
                module.build_brightdata_google_shopping_url("wagyu beef Tesco"),
                "test-zone",
            )],
        )
        self.assertEqual(first, second)
        self.assertEqual(first[0]["shop"], "Tesco")

    def test_pricesapi_lookup_uses_csv_shortlist_before_other_live_sources(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "community_supermarket_latest.csv"
            csv_path.write_text(
                "supermarket_name,price_gbp,price_unit_gbp,unit,product_name,normalized_product_name,capture_date,category_name,is_own_brand\n"
                "Tesco,4.5,13.24,kg,Tesco Finest Wagyu Burger 340G,tesco finest wagyu burger 340g,2024-04-12T00:00:00,fresh_food,true\n"
                "Aldi,3.49,10.26,kg,Specially Selected British Wagyu Beef Burgers 340g,specially selected british wagyu beef burgers 340g,2024-04-13T00:00:00,fresh_food,true\n"
                "Sains,3.0,8.83,kg,Finnebrogue Wagyu Beef Burgers x2 340g,finnebrogue wagyu beef burgers x2 340g,2024-04-13T00:00:00,fresh_food,false\n",
                encoding="utf-8",
            )
            old_path = module.COMMUNITY_DATASET_CSV_PATH
            old_search = module.fetch_pricesapi_search_results
            old_offers = module.fetch_pricesapi_product_offers
            old_brightdata = module.fetch_brightdata_shopping_results
            old_live = module.find_live_merchant_offers
            calls = []

            def fake_search(query: str):
                calls.append(("search", query))
                return [
                    {"id": "wagyu-1", "title": "Wagyu Beef Burgers", "offerCount": 2},
                    {"id": "gift-box", "title": "Luxury Wagyu Gift Box", "offerCount": 1},
                ]

            def fake_offers(product_id):
                calls.append(("offers", str(product_id)))
                if str(product_id) == "wagyu-1":
                    return [
                        {
                            "productTitle": "Tesco Finest Wagyu Burger 340G",
                            "price": "4.75",
                            "currency": "GBP",
                            "seller": "Tesco",
                            "url": "https://www.tesco.com/groceries/en-GB/products/314289933",
                        },
                        {
                            "productTitle": "Specially Selected British Wagyu Beef Burgers 340g",
                            "price": "3.59",
                            "currency": "GBP",
                            "seller": "ALDI UK",
                            "url": "https://www.aldi.co.uk/product/specially-selected-british-wagyu-beef-burgers-340g",
                        },
                    ]
                return [
                    {
                        "productTitle": "Luxury Wagyu Gift Box",
                        "price": "59.99",
                        "currency": "GBP",
                        "seller": "Luxury Meat Gifts",
                        "url": "https://example.com/gift-box",
                    }
                ]

            module.COMMUNITY_DATASET_CSV_PATH = csv_path
            module.fetch_pricesapi_search_results = fake_search
            module.fetch_pricesapi_product_offers = fake_offers
            module.fetch_brightdata_shopping_results = lambda query: (_ for _ in ()).throw(AssertionError(query))
            module.find_live_merchant_offers = lambda plan, search_terms: []
            module.os.environ["SPRINGFIELD_PRICE_PRICESAPI_KEY"] = "pricesapi-test-key"
            try:
                result = module.analyze_payload("Where to buy UK wagyu beef with best price?")
            finally:
                module.COMMUNITY_DATASET_CSV_PATH = old_path
                module.fetch_pricesapi_search_results = old_search
                module.fetch_pricesapi_product_offers = old_offers
                module.fetch_brightdata_shopping_results = old_brightdata
                module.find_live_merchant_offers = old_live
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.source, "PricesAPI live offers")
        self.assertEqual(calls, [("search", "wagyu beef"), ("offers", "wagyu-1"), ("offers", "gift-box")])
        self.assertIn("CSV-shortlisted retailers: Sainsbury's, Aldi, Tesco", result.reply_message)
        self.assertIn("Latest live offers from PricesAPI", result.reply_message)
        self.assertIn("Tesco: Tesco Finest Wagyu Burger 340G - £4.75", result.reply_message)
        self.assertIn("Aldi: Specially Selected British Wagyu Beef Burgers 340g - £3.59", result.reply_message)
        self.assertIn("Live results not matched for: Sainsbury's", result.reply_message)
        self.assertIn("PricesAPI checks live offers", result.reply_message)

    def test_retailer_search_pages_precede_pricesapi(self):
        fine_food_html = """
        <script>
        var meta = {"products":[{"handle":"wagyu-short-rib","variants":[{"price":1895,"name":"Wagyu Beef Short Rib, BMS 4-5, Frozen - 650g"}]}]};
        </script>
        """
        old_fetch = module.fetch_url
        old_fetch_json = module.fetch_json_url
        old_path = module.COMMUNITY_DATASET_CSV_PATH
        old_search = module.fetch_pricesapi_search_results
        old_offers = module.fetch_pricesapi_product_offers
        module.COMMUNITY_DATASET_CSV_PATH = ROOT / "tests" / "fixtures" / "missing-community.csv"
        module.os.environ["SPRINGFIELD_PRICE_PRICESAPI_KEY"] = "pricesapi-test-key"

        def fake_fetch(url: str):
            if "finefoodspecialist.co.uk" in url:
                return fine_food_html, url
            if "tomhixson.co.uk" in url:
                return "", url
            raise AssertionError(url)

        module.fetch_url = fake_fetch
        module.fetch_json_url = lambda url: ({"products": []}, url)
        module.fetch_pricesapi_search_results = lambda query: (_ for _ in ()).throw(AssertionError(query))
        module.fetch_pricesapi_product_offers = lambda product_id: (_ for _ in ()).throw(AssertionError(product_id))
        try:
            result = module.analyze_payload("Where to buy UK wagyu beef with best price?")
        finally:
            module.fetch_url = old_fetch
            module.fetch_json_url = old_fetch_json
            module.COMMUNITY_DATASET_CSV_PATH = old_path
            module.fetch_pricesapi_search_results = old_search
            module.fetch_pricesapi_product_offers = old_offers
        self.assertTrue(result.ok)
        self.assertEqual(result.source, "Retailer search pages")
        self.assertIn("Best matched live offers from retailer search pages", result.reply_message)
        self.assertIn("Comparison basis: standardized £/kg, then shelf price", result.reply_message)
        self.assertIn("Fine Food Specialist: Wagyu Beef Short Rib, BMS 4-5, Frozen - 650g - £18.95 (£29.15/kg)", result.reply_message)

    def test_costco_json_search_uses_product_page_price_fallback(self):
        costco_url = "https://www.costco.co.uk/Grocery-Household/Chilled-Foods/Meat-Meat-Boxes/Taste-Tradition-Wagyu-Beef-Burgers-24-x-170g-6oz/p/305939"
        costco_product_html = f"""
        <html>
          <head>
            <meta property="product:price:amount" content="69.99">
            <meta property="product:price:currency" content="GBP">
          </head>
          <body>
            <div class="price-per-unit">£17.16/kg</div>
            <script id="schemaorg_product" type="application/ld+json">
            {{
              "@context": "http://schema.org",
              "@type": "Product",
              "name": "Taste Tradition Wagyu Beef Burgers, 24 x 170g (6oz)",
              "offers": {{
                "@type": "Offer",
                "price": "69.99",
                "priceCurrency": "GBP",
                "availability": "http://schema.org/OutOfStock",
                "url": "{costco_url}"
              }}
            }}
            </script>
          </body>
        </html>
        """
        old_fetch = module.fetch_url
        old_fetch_json = module.fetch_json_url
        old_path = module.COMMUNITY_DATASET_CSV_PATH
        old_search = module.fetch_pricesapi_search_results
        old_offers = module.fetch_pricesapi_product_offers
        old_brightdata = module.fetch_brightdata_shopping_results
        seen_urls = []
        module.COMMUNITY_DATASET_CSV_PATH = ROOT / "tests" / "fixtures" / "missing-community.csv"

        def fake_fetch_json(url: str):
            seen_urls.append(url)
            if "costco.co.uk/rest/v2/uk/products/search" not in url:
                raise AssertionError(url)
            return (
                {
                    "products": [
                        {
                            "name": "Taste Tradition Wagyu Beef Burgers, 24 x 170g (6oz)",
                            "url": "/Grocery-Household/Chilled-Foods/Meat-Meat-Boxes/Taste-Tradition-Wagyu-Beef-Burgers-24-x-170g-6oz/p/305939",
                        }
                    ]
                },
                url,
            )

        def fake_fetch(url: str):
            if "finefoodspecialist.co.uk" in url or "tomhixson.co.uk" in url:
                return "", url
            if url == costco_url:
                return costco_product_html, url
            raise AssertionError(url)

        module.fetch_json_url = fake_fetch_json
        module.fetch_url = fake_fetch
        module.fetch_pricesapi_search_results = lambda query: (_ for _ in ()).throw(AssertionError(query))
        module.fetch_pricesapi_product_offers = lambda product_id: (_ for _ in ()).throw(AssertionError(product_id))
        module.fetch_brightdata_shopping_results = lambda query: (_ for _ in ()).throw(AssertionError(query))
        try:
            result = module.analyze_payload("Where to buy UK wagyu beef with best price?")
        finally:
            module.fetch_url = old_fetch
            module.fetch_json_url = old_fetch_json
            module.COMMUNITY_DATASET_CSV_PATH = old_path
            module.fetch_pricesapi_search_results = old_search
            module.fetch_pricesapi_product_offers = old_offers
            module.fetch_brightdata_shopping_results = old_brightdata
        self.assertTrue(result.ok)
        self.assertEqual(result.source, "Retailer search pages")
        self.assertEqual(
            seen_urls,
            ["https://www.costco.co.uk/rest/v2/uk/products/search?query=wagyu+beef&fields=FULL"],
        )
        self.assertIn("Best matched live offers from retailer search pages", result.reply_message)
        self.assertIn("Comparison basis: standardized £/kg, then shelf price", result.reply_message)
        self.assertIn("Costco: Taste Tradition Wagyu Beef Burgers, 24 x 170g (6oz) - £69.99 (£17.16/kg)", result.reply_message)
        self.assertIn(f"Link: {costco_url}", result.reply_message)

    def test_costco_requested_retailer_uses_live_search(self):
        old_fetch = module.fetch_url
        old_fetch_json = module.fetch_json_url
        old_path = module.COMMUNITY_DATASET_CSV_PATH
        old_search = module.fetch_pricesapi_search_results
        old_offers = module.fetch_pricesapi_product_offers
        old_brightdata = module.fetch_brightdata_shopping_results
        seen_urls = []
        module.COMMUNITY_DATASET_CSV_PATH = ROOT / "tests" / "fixtures" / "missing-community.csv"

        def fake_fetch_json(url: str):
            seen_urls.append(url)
            return (
                {
                    "products": [
                        {
                            "name": "Taste Tradition Wagyu Beef Burgers, 24 x 170g (6oz)",
                            "price": {"value": 69.99},
                            "url": "/Grocery-Household/Chilled-Foods/Meat-Meat-Boxes/Taste-Tradition-Wagyu-Beef-Burgers-24-x-170g-6oz/p/305939",
                        }
                    ]
                },
                url,
            )

        module.fetch_json_url = fake_fetch_json
        module.fetch_url = lambda url: (_ for _ in ()).throw(AssertionError(url))
        module.fetch_pricesapi_search_results = lambda query: (_ for _ in ()).throw(AssertionError(query))
        module.fetch_pricesapi_product_offers = lambda product_id: (_ for _ in ()).throw(AssertionError(product_id))
        module.fetch_brightdata_shopping_results = lambda query: (_ for _ in ()).throw(AssertionError(query))
        try:
            result = module.analyze_payload("What is the Costco wagyu beef price?")
        finally:
            module.fetch_url = old_fetch
            module.fetch_json_url = old_fetch_json
            module.COMMUNITY_DATASET_CSV_PATH = old_path
            module.fetch_pricesapi_search_results = old_search
            module.fetch_pricesapi_product_offers = old_offers
            module.fetch_brightdata_shopping_results = old_brightdata
        self.assertTrue(result.ok)
        self.assertEqual(result.source, "Retailer search pages")
        self.assertEqual(
            seen_urls,
            ["https://www.costco.co.uk/rest/v2/uk/products/search?query=wagyu+beef&fields=FULL"],
        )
        self.assertIn("Retailers mentioned: Costco", result.reply_message)
        self.assertIn("Comparison basis: standardized £/kg, then shelf price", result.reply_message)
        self.assertIn("Costco: Taste Tradition Wagyu Beef Burgers, 24 x 170g (6oz) - £69.99 (£17.15/kg)", result.reply_message)

    def test_live_offer_derives_each_price_from_pack_count(self):
        offer = module.collect_live_offer(
            "Costco",
            "Shen Dan Boiled Salted Duck Eggs x 12",
            5.49,
            "https://www.costco.co.uk/Grocery-Household/Food-Cupboard/Shen-Dan-Boiled-Salted-Duck-Eggs-x-12/p/8523735",
            ["shen", "dan", "duck"],
        )
        self.assertIsNotNone(offer)
        self.assertEqual(offer["unit"], "each")
        self.assertAlmostEqual(offer["price_unit_gbp"], 0.46, places=2)

    def test_brightdata_lookup_uses_csv_shortlist_before_live_shopping(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "community_supermarket_latest.csv"
            csv_path.write_text(
                "supermarket_name,price_gbp,price_unit_gbp,unit,product_name,normalized_product_name,capture_date,category_name,is_own_brand\n"
                "Sains,3.0,8.83,kg,Finnebrogue Wagyu Beef Burgers x2 340g,finnebrogue wagyu beef burgers x2 340g,2024-04-13T00:00:00,fresh_food,false\n"
                "Aldi,3.49,10.26,kg,Specially Selected British Wagyu Beef Burgers 340g,specially selected british wagyu beef burgers 340g,2024-04-13T00:00:00,fresh_food,true\n"
                "Tesco,4.5,13.24,kg,Tesco Finest Wagyu Burger 340G,tesco finest wagyu burger 340g,2024-04-12T00:00:00,fresh_food,true\n",
                encoding="utf-8",
            )
            old_path = module.COMMUNITY_DATASET_CSV_PATH
            old_fetch = module.fetch_brightdata_shopping_results
            old_live = module.find_live_merchant_offers
            seen_queries = []

            def fake_fetch(query: str):
                seen_queries.append(query)
                if "Sainsbury" in query:
                    return [
                        {
                            "title": "Finnebrogue Wagyu Beef Burgers x2 340g",
                            "price": "£3.25",
                            "shop": "Sainsbury's",
                            "link": "https://www.sainsburys.co.uk/gol-ui/product/sainsburys-2-wagyu-beef-burger-taste-the-difference-340g",
                        }
                    ]
                if "Aldi" in query:
                    return [
                        {
                            "title": "Specially Selected British Wagyu Beef Burgers 340g",
                            "price": "£3.59",
                            "shop": "ALDI UK",
                            "link": "https://www.aldi.co.uk/product/specially-selected-british-wagyu-beef-burgers-340g",
                        }
                    ]
                return [
                    {
                        "title": "Luxury Wagyu Gift Box",
                        "price": "£59.99",
                        "shop": "Luxury Meat Gifts",
                        "link": "https://example.com/gift-box",
                    }
                ]

            module.COMMUNITY_DATASET_CSV_PATH = csv_path
            module.fetch_brightdata_shopping_results = fake_fetch
            module.find_live_merchant_offers = lambda plan, search_terms: []
            module.os.environ["SPRINGFIELD_PRICE_BRIGHTDATA_API_KEY"] = "test-key"
            module.os.environ["SPRINGFIELD_PRICE_BRIGHTDATA_SERP_ZONE"] = "test-zone"
            try:
                result = module.analyze_payload("Where to buy UK wagyu beef with best price?")
            finally:
                module.COMMUNITY_DATASET_CSV_PATH = old_path
                module.fetch_brightdata_shopping_results = old_fetch
                module.find_live_merchant_offers = old_live
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.source, "Bright Data Google Shopping")
        self.assertEqual(
            seen_queries,
            [
                "wagyu beef Sainsbury's",
                "wagyu beef Aldi",
                "wagyu beef Tesco",
            ],
        )
        self.assertIn("CSV-shortlisted retailers: Sainsbury's, Aldi, Tesco", result.reply_message)
        self.assertIn("Latest live offers from Bright Data Google Shopping", result.reply_message)
        self.assertIn("Sainsbury's: Finnebrogue Wagyu Beef Burgers x2 340g - £3.25", result.reply_message)
        self.assertIn("Aldi: Specially Selected British Wagyu Beef Burgers 340g - £3.59", result.reply_message)
        self.assertIn("Live results not matched for: Tesco", result.reply_message)
        self.assertIn("Bright Data checks fresher Google Shopping offers", result.reply_message)


    def test_live_merchant_lookup_returns_fresher_matches(self):
        tom_hixson_html = """
        <script>
        _WLFDN.shopify.product_data.push({
          "handle": "rosendale-wagyu-roasting-joint",
          "item_name": "Rosendale Wagyu Roasting Joint",
          "price": "25.95"
        });
        </script>
        """
        fine_food_html = """
        <script>
        var meta = {"products":[{"handle":"wagyu-short-rib","variants":[{"price":1895,"name":"Wagyu Beef Short Rib, BMS 4-5, Frozen - 650g"}]}]};
        </script>
        """
        old_fetch = module.fetch_url
        old_path = module.COMMUNITY_DATASET_CSV_PATH
        module.COMMUNITY_DATASET_CSV_PATH = ROOT / "tests" / "fixtures" / "missing-community.csv"

        def fake_fetch(url: str):
            if "tomhixson.co.uk" in url:
                return tom_hixson_html, url
            if "finefoodspecialist.co.uk" in url:
                return fine_food_html, url
            raise AssertionError(url)

        module.fetch_url = fake_fetch
        try:
            result = module.analyze_payload("Where to buy UK wagyu beef with best price?")
        finally:
            module.fetch_url = old_fetch
            module.COMMUNITY_DATASET_CSV_PATH = old_path
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.source, "Retailer search pages")
        self.assertIn("Best matched live offers from retailer search pages", result.reply_message)
        self.assertIn("Fine Food Specialist: Wagyu Beef Short Rib, BMS 4-5, Frozen - 650g - £18.95", result.reply_message)
        self.assertIn("Link: https://www.finefoodspecialist.co.uk/products/wagyu-short-rib", result.reply_message)
        self.assertIn("live retailer search-page matches", result.reply_message)

    def test_csv_snapshot_lookup_returns_best_matches_when_live_search_unavailable(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "community_supermarket_latest.csv"
            csv_path.write_text(
                "supermarket_name,price_gbp,price_unit_gbp,unit,product_name,normalized_product_name,capture_date,category_name,is_own_brand\n"
                "Tesco,6.5,13,kg,Tesco Finest Wagyu Beef Burger,tesco finest wagyu beef burger,2024-04-10T00:00:00,frozen,true\n"
                "ASDA,7.0,14,kg,ASDA Wagyu Beef Burger,asda wagyu beef burger,2024-04-09T00:00:00,frozen,true\n",
                encoding="utf-8",
            )
            old_path = module.COMMUNITY_DATASET_CSV_PATH
            old_fetch = module.fetch_url
            module.COMMUNITY_DATASET_CSV_PATH = csv_path
            module.fetch_url = lambda url: (_ for _ in ()).throw(RuntimeError("offline"))
            try:
                result = module.analyze_payload("Where to buy UK wagyu beef with best price?")
            finally:
                module.COMMUNITY_DATASET_CSV_PATH = old_path
                module.fetch_url = old_fetch
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.source, "Community UK supermarket time-series dataset")
        self.assertIn("Tesco Finest Wagyu Beef Burger - £6.50 (£13.00/kg)", result.reply_message)
        self.assertIn("Uses a locally converted CSV snapshot", result.reply_message)

    def test_csv_snapshot_lookup_filters_to_named_retailers(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "community_supermarket_latest.csv"
            csv_path.write_text(
                "supermarket_name,price_gbp,price_unit_gbp,unit,product_name,normalized_product_name,capture_date,category_name,is_own_brand\n"
                "Tesco,2.6,5.2,dozen,Tesco Eggs,tesco eggs,2024-04-10T00:00:00,fresh_food,true\n"
                "ASDA,2.4,4.8,dozen,ASDA Eggs,asda eggs,2024-04-11T00:00:00,fresh_food,true\n"
                "Morrisons,2.2,4.4,dozen,Morrisons Eggs,morrisons eggs,2024-04-12T00:00:00,fresh_food,true\n",
                encoding="utf-8",
            )
            old_path = module.COMMUNITY_DATASET_CSV_PATH
            module.COMMUNITY_DATASET_CSV_PATH = csv_path
            try:
                result = module.analyze_payload("Where can I compare the best value eggs across Tesco and ASDA this week?")
            finally:
                module.COMMUNITY_DATASET_CSV_PATH = old_path
        self.assertTrue(result.ok)
        self.assertIn("Retailers mentioned: Tesco, ASDA", result.reply_message)
        self.assertIn("Tesco Eggs - £2.60 (£5.20/dozen)", result.reply_message)
        self.assertIn("ASDA Eggs - £2.40 (£4.80/dozen)", result.reply_message)
        self.assertNotIn("Morrisons Eggs", result.reply_message)

    def test_box_of_12_eggs_prefers_real_egg_packs(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "community_supermarket_latest.csv"
            csv_path.write_text(
                "supermarket_name,price_gbp,price_unit_gbp,unit,product_name,normalized_product_name,capture_date,category_name,is_own_brand\n"
                "ASDA,2.4,4.8,dozen,ASDA Eggs,asda eggs,2024-04-11T00:00:00,fresh_food,true\n"
                "Tesco,3.5,7.0,kg,Cadbury Creme Eggs 48 x 40g,cadbury creme eggs 48 x 40g,2024-04-10T00:00:00,food_cupboard,false\n"
                "Sains,5.0,5.0,unit,A Dozen Red Roses,a dozen red roses,2024-04-09T00:00:00,fresh_food,false\n",
                encoding="utf-8",
            )
            old_path = module.COMMUNITY_DATASET_CSV_PATH
            old_fetch = module.fetch_url
            module.COMMUNITY_DATASET_CSV_PATH = csv_path
            module.fetch_url = lambda url: (_ for _ in ()).throw(RuntimeError("offline"))
            try:
                result = module.analyze_payload("I want to buy a box of 12 eggs")
            finally:
                module.COMMUNITY_DATASET_CSV_PATH = old_path
                module.fetch_url = old_fetch
        self.assertTrue(result.ok)
        self.assertEqual(result.source, "Community UK supermarket time-series dataset")
        self.assertIn("Search terms: eggs", result.reply_message)
        self.assertIn("ASDA Eggs - £2.40 (£4.80/dozen)", result.reply_message)
        self.assertNotIn("Cadbury Creme Eggs", result.reply_message)
        self.assertNotIn("A Dozen Red Roses", result.reply_message)

    def test_csv_snapshot_skips_non_food_categories(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "community_supermarket_latest.csv"
            csv_path.write_text(
                "supermarket_name,price_gbp,price_unit_gbp,unit,product_name,normalized_product_name,capture_date,category_name,is_own_brand\n"
                "ASDA,2.0,0.08,unit,George Easter Fillable Eggs,george easter fillable eggs,2024-03-30T00:00:00,home,true\n"
                "ASDA,2.4,4.8,dozen,ASDA Eggs,asda eggs,2024-04-11T00:00:00,fresh_food,true\n",
                encoding="utf-8",
            )
            old_path = module.COMMUNITY_DATASET_CSV_PATH
            module.COMMUNITY_DATASET_CSV_PATH = csv_path
            try:
                result = module.analyze_payload("Where can I compare the best value eggs across ASDA this week?")
            finally:
                module.COMMUNITY_DATASET_CSV_PATH = old_path
        self.assertTrue(result.ok)
        self.assertIn("ASDA Eggs - £2.40 (£4.80/dozen)", result.reply_message)
        self.assertNotIn("George Easter Fillable Eggs", result.reply_message)

    def test_official_trend_query_filters_to_official_sources(self):
        result = module.analyze_payload("I only want official UK sources for food price inflation trends.")
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.source, "ONS Shopping Prices Comparison Tool")
        self.assertIn("ONS Shopping Prices Comparison Tool", result.reply_message)
        self.assertIn("Defra Food Statistics Pocketbook", result.reply_message)
        self.assertNotIn("Community UK supermarket time-series dataset", result.reply_message)

    def test_location_query_surfaces_geolytix_with_caveat(self):
        result = module.analyze_payload("Where can I find nearby supermarkets in Sheffield before checking prices?")
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.query_type, "store_location")
        self.assertEqual(result.source, "Geolytix supermarket location data")
        self.assertIn("Geolytix supermarket location data", result.reply_message)
        self.assertIn("Search anchor: Sheffield", result.reply_message)
        self.assertIn("does not publish prices", result.reply_message)

    def test_location_query_defaults_to_pa20sg(self):
        result = module.analyze_payload("Show nearby supermarkets.")
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "query")
        self.assertEqual(result.query_type, "store_location")
        self.assertIn("Default postcode: PA2 0SG", result.reply_message)

    def test_fixture_extracts_price_range(self):
        result = module.analyze_payload(str(FIXTURE))
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "product")
        self.assertEqual(result.product_name, "Faux Leather Ottoman Storage Divan Base")
        self.assertEqual(result.currency, "GBP")
        self.assertEqual(result.low_price, 274.0)
        self.assertEqual(result.high_price, 494.0)
        self.assertEqual(result.regular_low_price, 586.0)
        self.assertEqual(result.regular_high_price, 775.0)
        self.assertEqual(result.availability, "InStock")
        self.assertIn("Current price: £274.00 to £494.00", result.reply_message)

    def test_sainsburys_product_page_fallback_uses_price_per_unit_selector(self):
        url = "https://www.sainsburys.co.uk/gol-ui/product/sainsburys-free-range-eggs"
        html = """
        <html>
          <head><title>Sainsbury's British Free Range Eggs</title></head>
          <body>
            <p class="pricePerUnit">50p/each</p>
          </body>
        </html>
        """
        old_fetch = module.fetch_url
        module.fetch_url = lambda incoming_url: (html, incoming_url)
        try:
            result = module.analyze_payload(url)
        finally:
            module.fetch_url = old_fetch
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "product")
        self.assertEqual(result.currency, "GBP")
        self.assertEqual(result.current_price, 0.5)
        self.assertIn("Current price: £0.50", result.reply_message)

    def test_morrisons_product_page_fallback_uses_nowprice_or_typicalprice(self):
        url = "https://groceries.morrisons.com/products/morrisons-milk"
        html = """
        <html>
          <head><title>Morrisons Semi Skimmed Milk</title></head>
          <body>
            <div class="related-search-ribbon-enabled">
              <span class="typicalPrice">£1.70</span>
            </div>
          </body>
        </html>
        """
        old_fetch = module.fetch_url
        module.fetch_url = lambda incoming_url: (html, incoming_url)
        try:
            result = module.analyze_payload(url)
        finally:
            module.fetch_url = old_fetch
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "product")
        self.assertEqual(result.currency, "GBP")
        self.assertEqual(result.current_price, 1.7)
        self.assertIn("Current price: £1.70", result.reply_message)

    def test_tesco_product_page_fallback_uses_value_selector(self):
        url = "https://www.tesco.com/groceries/en-GB/products/123456789"
        html = """
        <html>
          <head><title>Tesco Pasta</title></head>
          <body>
            <span class="value">2.50</span>
          </body>
        </html>
        """
        old_fetch = module.fetch_url
        module.fetch_url = lambda incoming_url: (html, incoming_url)
        try:
            result = module.analyze_payload(url)
        finally:
            module.fetch_url = old_fetch
        self.assertTrue(result.ok)
        self.assertEqual(result.mode, "product")
        self.assertEqual(result.currency, "GBP")
        self.assertEqual(result.current_price, 2.5)
        self.assertIn("Current price: £2.50", result.reply_message)

    def test_error_for_empty_payload(self):
        result = module.analyze_payload("")
        self.assertFalse(result.ok)
        self.assertIn("UK food price question", result.error_message)

    def test_disallow_local_files_when_disabled(self):
        old = module.os.environ.get("SPRINGFIELD_PRICE_ALLOW_LOCAL_FILES")
        module.os.environ["SPRINGFIELD_PRICE_ALLOW_LOCAL_FILES"] = "0"
        try:
            result = module.analyze_payload(str(FIXTURE))
        finally:
            if old is None:
                module.os.environ.pop("SPRINGFIELD_PRICE_ALLOW_LOCAL_FILES", None)
            else:
                module.os.environ["SPRINGFIELD_PRICE_ALLOW_LOCAL_FILES"] = old
        self.assertFalse(result.ok)
        self.assertIn("disabled", result.error_message)


if __name__ == "__main__":
    unittest.main()
