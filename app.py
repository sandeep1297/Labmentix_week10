import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
from datetime import date

# Replace with your MySQL connection details
db_config = {
    'host': 'localhost',
    'database': 'food_wastage_db',
    'user': 'root',
    'password': 'sandeep',
    'port': 3306
}

@st.cache_resource
def create_sqlalchemy_engine(config):
    """Create a SQLAlchemy engine for the MySQL database."""
    try:
        engine = create_engine(
            f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        )
        return engine
    except Exception as e:
        st.error(f"Error creating SQLAlchemy engine: {e}")
        return None

# Create the engine
engine = create_sqlalchemy_engine(db_config)

if not engine:
    st.stop()

def get_data_from_db(query, engine, params=None):
    """A helper function to run SQL queries and return a DataFrame."""
    try:
        df = pd.read_sql_query(query, engine, params=params)
        return df
    except Exception as e:
        st.error(f"Failed to execute query: {e}")
        return pd.DataFrame()

# --- CRUD Functions ---
def create_food_listing(engine, food_name, quantity, expiry_date, provider_id, provider_type, location, food_type, meal_type):
    """Add a new food listing to the database using SQLAlchemy engine."""
    query = """
    INSERT INTO food_listings (Food_Name, Quantity, Expiry_Date, Provider_ID, Provider_Type, Location, Food_Type, Meal_Type)
    VALUES (:food_name, :quantity, :expiry_date, :provider_id, :provider_type, :location, :food_type, :meal_type)
    """
    data = {
        'food_name': food_name,
        'quantity': quantity,
        'expiry_date': expiry_date,
        'provider_id': provider_id,
        'provider_type': provider_type,
        'location': location,
        'food_type': food_type,
        'meal_type': meal_type
    }
    with engine.connect() as connection:
        try:
            connection.execute(text(query), data)
            connection.commit()
            st.success("Food listing created successfully!")
        except Exception as e:
            st.error(f"Failed to create listing: {e}")

def update_food_listing_quantity(engine, food_id, new_quantity):
    """Update the quantity of a food listing using SQLAlchemy engine."""
    query = "UPDATE food_listings SET Quantity = :new_quantity WHERE Food_ID = :food_id"
    with engine.connect() as connection:
        try:
            connection.execute(text(query), {'new_quantity': new_quantity, 'food_id': food_id})
            connection.commit()
            st.success("Food listing updated successfully!")
        except Exception as e:
            st.error(f"Failed to update listing: {e}")

def delete_food_listing(engine, food_id):
    """Delete a food listing from the database using SQLAlchemy engine."""
    query = "DELETE FROM food_listings WHERE Food_ID = :food_id"
    with engine.connect() as connection:
        try:
            connection.execute(text(query), {'food_id': food_id})
            connection.commit()
            st.success("Food listing deleted successfully!")
        except Exception as e:
            st.error(f"Failed to delete listing: {e}")

# Main application layout
st.set_page_config(page_title="Local Food Wastage Management System", layout="wide")
st.title("🍽️ Local Food Wastage Management System")
st.markdown("---")

# Sidebar for CRUD operations
st.sidebar.header("Data Management (CRUD)")
action = st.sidebar.radio("Choose an action", ["View", "Create", "Update", "Delete"])

if action == "View":
    # --- Section 1: Data Analysis Reports (15 SQL Queries) ---
    st.header("📊 Data Analysis and Reports")
    
    # Query 1: Number of providers and receivers in each city
    st.subheader("1. Providers and Receivers by City")
    providers_receivers_query = """
        SELECT
            T1.City,
            COUNT(DISTINCT T1.Provider_ID) AS Number_of_Providers,
            COUNT(DISTINCT T2.Receiver_ID) AS Number_of_Receivers
        FROM providers AS T1
        LEFT JOIN receivers AS T2 ON T1.City = T2.City
        GROUP BY T1.City
        ORDER BY Number_of_Providers DESC, Number_of_Receivers DESC;
    """
    st.dataframe(get_data_from_db(text(providers_receivers_query), engine).head(15))

    # Query 2: Which city has the highest number of food listings?
    st.subheader("2. City with the Highest Number of Listings")
    listings_by_city_query = """
        SELECT Location, COUNT(*) as Number_of_Listings
        FROM food_listings
        GROUP BY Location
        ORDER BY Number_of_Listings DESC
        LIMIT 1;
    """
    st.dataframe(get_data_from_db(text(listings_by_city_query), engine))
    
    # Query 3: What is the total quantity of food available from all providers?
    st.subheader("3. Total Food Available")
    total_quantity_query = "SELECT SUM(Quantity) as Total_Available_Quantity FROM food_listings;"
    st.dataframe(get_data_from_db(text(total_quantity_query), engine))

    # Query 4: Most claimed food items
    st.subheader("4. Most Claimed Food Items")
    most_claimed_food_query = """
        SELECT
            fl.Food_Name,
            COUNT(c.Claim_ID) AS Number_of_Claims
        FROM claims AS c
        JOIN food_listings AS fl ON c.Food_ID = fl.Food_ID
        GROUP BY fl.Food_Name
        ORDER BY Number_of_Claims DESC
        LIMIT 5;
    """
    st.dataframe(get_data_from_db(text(most_claimed_food_query), engine))

    # Query 5: Which provider has the highest number of successful claims?
    st.subheader("5. Provider with the Highest Successful Claims")
    successful_claims_query = """
        SELECT p.Name, COUNT(c.Claim_ID) AS Successful_Claims
        FROM providers AS p
        JOIN food_listings AS fl ON p.Provider_ID = fl.Provider_ID
        JOIN claims AS c ON fl.Food_ID = c.Food_ID
        WHERE c.Status = 'Completed'
        GROUP BY p.Name
        ORDER BY Successful_Claims DESC
        LIMIT 1;
    """
    st.dataframe(get_data_from_db(text(successful_claims_query), engine))

    # Query 6: Percentage of claims by status
    st.subheader("6. Claim Status Percentage")
    claims_percentage_query = """
        SELECT
            Status,
            CAST(COUNT(*) AS REAL) * 100 / (SELECT COUNT(*) FROM claims) AS Percentage
        FROM claims
        GROUP BY Status;
    """
    st.dataframe(get_data_from_db(text(claims_percentage_query), engine))

    # Query 7: Average quantity of food claimed per receiver
    st.subheader("7. Average Quantity Claimed Per Receiver")
    avg_quantity_query = """
        SELECT AVG(fl.Quantity) AS Average_Quantity_Claimed_Per_Receiver
        FROM claims AS c
        JOIN food_listings AS fl ON c.Food_ID = fl.Food_ID;
    """
    st.dataframe(get_data_from_db(text(avg_quantity_query), engine))

    # Query 8: Most claimed meal type
    st.subheader("8. Most Claimed Meal Type")
    claimed_meal_type_query = """
        SELECT fl.Meal_Type, COUNT(c.Claim_ID) AS Number_of_Claims
        FROM claims AS c
        JOIN food_listings AS fl ON c.Food_ID = fl.Food_ID
        GROUP BY fl.Meal_Type
        ORDER BY Number_of_Claims DESC;
    """
    st.dataframe(get_data_from_db(text(claimed_meal_type_query), engine))

    # Query 9: Most commonly available food types
    st.subheader("9. Most Commonly Available Food Types")
    food_types_query = """
        SELECT Food_Type, COUNT(*) as Number_of_Items
        FROM food_listings
        GROUP BY Food_Type
        ORDER BY Number_of_Items DESC;
    """
    st.dataframe(get_data_from_db(text(food_types_query), engine))

    # Query 10: Total quantity of food donated by each provider
    st.subheader("10. Total Quantity Donated by Each Provider (Top 10)")
    total_donations_query = """
        SELECT
            p.Name,
            SUM(fl.Quantity) as Total_Quantity
        FROM
            providers as p
        JOIN
            food_listings as fl ON p.Provider_ID = fl.Provider_ID
        GROUP BY
            p.Name
        ORDER BY
            Total_Quantity DESC
        LIMIT 10;
    """
    st.dataframe(get_data_from_db(text(total_donations_query), engine))

    # Query 11: Which provider has the highest number of successful food claims? (re-run for completeness)
    st.subheader("11. Provider with the Highest Number of Successful Claims")
    query_11 = """
    SELECT p.Name, COUNT(c.Claim_ID) AS Successful_Claims
    FROM providers AS p
    JOIN food_listings AS fl ON p.Provider_ID = fl.Provider_ID
    JOIN claims AS c ON fl.Food_ID = c.Food_ID
    WHERE c.Status = 'Completed'
    GROUP BY p.Name
    ORDER BY Successful_Claims DESC
    LIMIT 1;
    """
    st.dataframe(get_data_from_db(text(query_11), engine))

    # Query 12: How many claims per food item (re-run for completeness)
    st.subheader("12. Number of Claims per Food Item")
    query_12 = """
    SELECT fl.Food_Name, COUNT(c.Claim_ID) as Number_of_Claims
    FROM food_listings AS fl
    JOIN claims AS c ON fl.Food_ID = c.Food_ID
    GROUP BY fl.Food_Name
    ORDER BY Number_of_Claims DESC;
    """
    st.dataframe(get_data_from_db(text(query_12), engine).head(15))

    # Query 13: Top receivers with the most claims (re-run for completeness)
    st.subheader("13. Receivers with the Most Claims (Top 10)")
    query_13 = """
    SELECT r.Name, COUNT(c.Claim_ID) AS Total_Claims
    FROM receivers AS r
    JOIN claims AS c ON r.Receiver_ID = c.Receiver_ID
    GROUP BY r.Name
    ORDER BY Total_Claims DESC
    LIMIT 10;
    """
    st.dataframe(get_data_from_db(text(query_13), engine))

    # Query 14: Total claims
    st.subheader("14. Total Claims Made")
    query_14 = "SELECT COUNT(*) AS Total_Claims FROM claims;"
    st.dataframe(get_data_from_db(text(query_14), engine))

    # Query 15: Total donations
    st.subheader("15. Total Donations Made")
    query_15 = "SELECT COUNT(*) AS Total_Donations FROM food_listings;"
    st.dataframe(get_data_from_db(text(query_15), engine))

    # --- Section 2: Filtering and Interactive Display ---
    st.markdown("---")
    st.header("🔍 Filter and Find Food")

    # Get unique values for filters
    cities = get_data_from_db("SELECT DISTINCT City FROM providers ORDER BY City", engine)['City'].tolist()
    provider_types = get_data_from_db("SELECT DISTINCT Type FROM providers ORDER BY Type", engine)['Type'].tolist()
    food_types = get_data_from_db("SELECT DISTINCT Food_Type FROM food_listings ORDER BY Food_Type", engine)['Food_Type'].tolist()
    meal_types = get_data_from_db("SELECT DISTINCT Meal_Type FROM food_listings ORDER BY Meal_Type", engine)['Meal_Type'].tolist()

    col1, col2 = st.columns(2)
    with col1:
        selected_city = st.selectbox("Filter by City", ["All"] + cities)
        selected_provider_type = st.selectbox("Filter by Provider Type", ["All"] + provider_types)
    with col2:
        selected_food_type = st.selectbox("Filter by Food Type", ["All"] + food_types)
        selected_meal_type = st.selectbox("Filter by Meal Type", ["All"] + meal_types)

    # Build a dynamic query based on filters
    query_conditions = []
    params = {}

    if selected_city != "All":
        query_conditions.append("t1.Location = :city")
        params['city'] = selected_city
    if selected_provider_type != "All":
        query_conditions.append("t2.Type = :provider_type")
        params['provider_type'] = selected_provider_type
    if selected_food_type != "All":
        query_conditions.append("t1.Food_Type = :food_type")
        params['food_type'] = selected_food_type
    if selected_meal_type != "All":
        query_conditions.append("t1.Meal_Type = :meal_type")
        params['meal_type'] = selected_meal_type

    base_query = "SELECT t1.Food_ID, t1.Food_Name, t1.Quantity, t1.Expiry_Date, t1.Provider_Type, t1.Location, t1.Food_Type, t1.Meal_Type, t1.Provider_ID FROM food_listings AS t1 JOIN providers AS t2 ON t1.Provider_ID = t2.Provider_ID"
    if query_conditions:
        final_query = base_query + " WHERE " + " AND ".join(query_conditions)
    else:
        final_query = base_query

    filtered_listings_df = get_data_from_db(text(final_query), engine, params=params)
    st.subheader("Available Food Listings (Filtered)")
    st.dataframe(filtered_listings_df)

    # --- Section 3: Contact Details ---
    st.markdown("---")
    st.header("📞 Contact Providers for Coordination")
    if not filtered_listings_df.empty:
        provider_ids = filtered_listings_df['Provider_ID'].unique().tolist()
        if provider_ids:
            placeholders = ', '.join([f":p{i}" for i in range(len(provider_ids))])
            provider_details_query = f"SELECT Name, Contact, City FROM providers WHERE Provider_ID IN ({placeholders})"
            params = {f'p{i}': provider_id for i, provider_id in enumerate(provider_ids)}
            
            provider_details_df = get_data_from_db(text(provider_details_query), engine, params=params)
            
            if not provider_details_df.empty:
                st.subheader("Contact Information for Filtered Providers")
                st.dataframe(provider_details_df)
            else:
                st.info("No provider contact details found for the current filters.")
    else:
        st.info("No food listings found for the selected filters.")

elif action == "Create":
    st.header("➕ Add a New Food Listing")
    with st.form("create_form"):
        st.write("Enter details for the new food listing:")
        
        providers_df = get_data_from_db("SELECT Provider_ID, Name, Type, City FROM providers", engine)
        
        provider_name = st.selectbox("Select Provider", providers_df['Name'])
        selected_provider_info = providers_df[providers_df['Name'] == provider_name].iloc[0]
        
        food_name = st.text_input("Food Name")
        quantity = st.number_input("Quantity", min_value=1)
        expiry_date = st.date_input("Expiry Date", min_value=date.today())
        food_type = st.selectbox("Food Type", ["Vegetarian", "Non-Vegetarian", "Vegan"])
        meal_type = st.selectbox("Meal Type", ["Breakfast", "Lunch", "Dinner", "Snacks"])

        submitted = st.form_submit_button("Add Listing")
        if submitted:
            create_food_listing(
                engine=engine,
                food_name=food_name,
                quantity=quantity,
                expiry_date=expiry_date,
                provider_id=selected_provider_info['Provider_ID'],
                provider_type=selected_provider_info['Type'],
                location=selected_provider_info['City'],
                food_type=food_type,
                meal_type=meal_type
            )
            st.experimental_rerun()

elif action == "Update":
    st.header("✏️ Update a Food Listing")
    with st.form("update_form"):
        st.write("Select a listing to update its quantity:")
        all_listings_df = get_data_from_db("SELECT Food_ID, Food_Name, Quantity, Location FROM food_listings ORDER BY Food_ID DESC", engine)
        st.dataframe(all_listings_df)
        
        food_id_to_update = st.number_input("Enter Food ID to Update", min_value=1)
        new_quantity = st.number_input("Enter New Quantity", min_value=1)
        
        submitted = st.form_submit_button("Update Quantity")
        if submitted:
            update_food_listing_quantity(engine, food_id_to_update, new_quantity)
            st.experimental_rerun()

elif action == "Delete":
    st.header("🗑️ Delete a Food Listing")
    with st.form("delete_form"):
        st.write("Select a listing to delete:")
        all_listings_df = get_data_from_db("SELECT Food_ID, Food_Name, Quantity, Location FROM food_listings ORDER BY Food_ID DESC", engine)
        st.dataframe(all_listings_df)
        
        food_id_to_delete = st.number_input("Enter Food ID to Delete", min_value=1)
        
        submitted = st.form_submit_button("Delete Listing")
        if submitted:
            delete_food_listing(engine, food_id_to_delete)
            st.experimental_rerun()