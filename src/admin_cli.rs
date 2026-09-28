use sqlx::{sqlite::SqlitePoolOptions, FromRow, SqlitePool};
use std::io::{self, Write};

#[derive(Debug, FromRow)]
pub struct CliUser {
    pub id: i64,
    pub username: String,
    pub display_name: String,
    pub role: String,
    pub all_download_access: i64,
    pub created_at: String,
}

#[derive(Debug, FromRow)]
pub struct CliProduct {
    pub id: i64,
    pub title: String,
    pub min_price: f64,
    pub artist_id: i64,
}

#[derive(Debug, FromRow)]
pub struct CliCommission {
    pub id: i64,
    pub title: String,
    pub client_id: i64,
    pub artist_id: i64,
    pub status: String,
    pub price: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("===========================================================");
    println!(" 🛡️  AtelierV Native Admin CLI (Terminal / Non-Web)");
    println!("    Direct SQLite Connection & Realtime State Inspector");
    println!("===========================================================\n");

    let db_path = "sqlite:vgen_local.db?mode=rwc";
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(db_path)
        .await?;

    loop {
        println!("\n[ADMIN MENU]");
        println!("  1. List All Users (with All-Download Access status)");
        println!("  2. Add New User Persona");
        println!("  3. Add Tester Account (All-Download Testing Access)");
        println!("  4. Toggle All-Download Access for Existing User");
        println!("  5. List All Digital Marketplace Products");
        println!("  6. List All Commissions & Workflows");
        println!("  7. Delete User");
        println!("  8. Exit");
        print!("\nSelect option (1-8): ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        match choice {
            "1" => list_users(&pool).await?,
            "2" => add_user(&pool, false).await?,
            "3" => add_user(&pool, true).await?,
            "4" => toggle_all_access(&pool).await?,
            "5" => list_products(&pool).await?,
            "6" => list_commissions(&pool).await?,
            "7" => delete_user(&pool).await?,
            "8" => {
                println!("Exiting AtelierV Admin CLI.");
                break;
            }
            _ => println!("Invalid option, please choose 1-8."),
        }
    }

    Ok(())
}

async fn list_users(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let users = sqlx::query_as::<_, CliUser>(
        "SELECT id, username, display_name, role, all_download_access, created_at FROM users ORDER BY id ASC"
    )
    .fetch_all(pool)
    .await?;

    println!("\n--- USER DIRECTORY ---");
    println!("{:<4} | {:<15} | {:<25} | {:<10} | {:<18}", "ID", "Username", "Display Name", "Role", "All-Download Pass");
    println!("{}", "-".repeat(80));

    for u in users {
        let access_str = if u.all_download_access == 1 { "YES (Tester)" } else { "No" };
        println!("{:<4} | {:<15} | {:<25} | {:<10} | {:<18}", u.id, u.username, u.display_name, u.role, access_str);
    }
    Ok(())
}

async fn add_user(pool: &SqlitePool, all_access: bool) -> Result<(), Box<dyn std::error::Error>> {
    print!("Username: ");
    io::stdout().flush()?;
    let mut username = String::new();
    io::stdin().read_line(&mut username)?;
    let username = username.trim().to_lowercase();

    print!("Display Name: ");
    io::stdout().flush()?;
    let mut display_name = String::new();
    io::stdin().read_line(&mut display_name)?;
    let display_name = display_name.trim();

    let mut role = "Client";
    if !all_access {
        print!("Role (Client, Artist, Admin) [Client]: ");
        io::stdout().flush()?;
        let mut r = String::new();
        io::stdin().read_line(&mut r)?;
        let r = r.trim();
        if r.eq_ignore_ascii_case("Artist") {
            role = "Artist";
        } else if r.eq_ignore_ascii_case("Admin") {
            role = "Admin";
        }
    }

    print!("Password [password123]: ");
    io::stdout().flush()?;
    let mut pass = String::new();
    io::stdin().read_line(&mut pass)?;
    let pass = pass.trim();
    let pass = if pass.is_empty() { "password123" } else { pass };

    let access_flag = if all_access { 1 } else { 0 };

    sqlx::query(
        "INSERT INTO users (username, display_name, role, password_hash, all_download_access) VALUES (?, ?, ?, ?, ?)"
    )
    .bind(&username)
    .bind(display_name)
    .bind(role)
    .bind(pass)
    .bind(access_flag)
    .execute(pool)
    .await?;

    println!("✓ Successfully created user @{} (All-Download: {})", username, all_access);
    Ok(())
}

async fn toggle_all_access(pool: &SqlitePool) -> Result<(), Box<dyn std::error::Error>> {
    print!("Enter User ID to toggle All-Download access: ");
    io::stdout().flush()?;
    let mut id_str = String::new();
    io::stdin().read_line(&mut id_str)?;
    let id: i64 = id_str.trim().parse().unwrap_or(0);

    let current: Option<(i64,)> = sqlx::query_as("SELECT all_download_access FROM users WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?;

    match current {
        Some((val,)) => {
            let next_val = if val == 1 { 0 } else { 1 };
            sqlx::query("UPDATE users SET all_download_access = ? WHERE id = ?")
                .bind(next_val)
                .bind(id)
                .execute(pool)
                .await?;
            println!("✓ User #{} All-Download testing access set to {}", id, next_val == 1);
        }
        None => println!("User not found."),
    }
    Ok(())
}

async fn list_products(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let prods = sqlx::query_as::<_, CliProduct>(
        "SELECT id, title, min_price, artist_id FROM digital_products ORDER BY id DESC"
    )
    .fetch_all(pool)
    .await?;

    println!("\n--- DIGITAL PRODUCTS MARKETPLACE ---");
    println!("{:<4} | {:<40} | {:<12} | {:<10}", "ID", "Title", "Min Price", "Artist ID");
    println!("{}", "-".repeat(75));

    for p in prods {
        println!("{:<4} | {:<40} | ${:<11.2} | {:<10}", p.id, p.title, p.min_price, p.artist_id);
    }
    Ok(())
}

async fn list_commissions(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let comms = sqlx::query_as::<_, CliCommission>(
        "SELECT id, title, client_id, artist_id, status, price FROM commissions ORDER BY id DESC"
    )
    .fetch_all(pool)
    .await?;

    println!("\n--- COMMISSIONS ---");
    println!("{:<4} | {:<35} | {:<10} | {:<10} | {:<14} | {:<10}", "ID", "Title", "Client ID", "Artist ID", "Status", "Price");
    println!("{}", "-".repeat(95));

    for c in comms {
        println!("{:<4} | {:<35} | {:<10} | {:<10} | {:<14} | ${:<9.2}", c.id, c.title, c.client_id, c.artist_id, c.status, c.price);
    }
    Ok(())
}

async fn delete_user(pool: &SqlitePool) -> Result<(), Box<dyn std::error::Error>> {
    print!("Enter User ID to delete: ");
    io::stdout().flush()?;
    let mut id_str = String::new();
    io::stdin().read_line(&mut id_str)?;
    let id: i64 = id_str.trim().parse().unwrap_or(0);

    sqlx::query("DELETE FROM users WHERE id = ?").bind(id).execute(pool).await?;
    println!("✓ User #{} deleted.", id);
    Ok(())
}
