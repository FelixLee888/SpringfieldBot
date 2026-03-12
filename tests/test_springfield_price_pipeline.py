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
        self.assertIn("Tesco Finest Wagyu Beef Burger - £6.50 (£13.00/kg)", result.reply_message)
        self.assertIn("ASDA Wagyu Beef Burger - £7.00 (£14.00/kg)", result.reply_message)

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
        module.fetch_pricesapi_search_results = lambda query: (_ for _ in ()).throw(AssertionError(query))
        module.fetch_pricesapi_product_offers = lambda product_id: (_ for _ in ()).throw(AssertionError(product_id))
        try:
            result = module.analyze_payload("Where to buy UK wagyu beef with best price?")
        finally:
            module.fetch_url = old_fetch
            module.COMMUNITY_DATASET_CSV_PATH = old_path
            module.fetch_pricesapi_search_results = old_search
            module.fetch_pricesapi_product_offers = old_offers
        self.assertTrue(result.ok)
        self.assertEqual(result.source, "Retailer search pages")
        self.assertIn("Best matched live offers from retailer search pages", result.reply_message)
        self.assertIn("Fine Food Specialist: Wagyu Beef Short Rib, BMS 4-5, Frozen - 650g - £18.95", result.reply_message)

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
