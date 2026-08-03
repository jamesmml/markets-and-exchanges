/*
 * compound_interest.c
 *
 * A simple compound interest calculator.
 * 
 * Author: James Milgram
 */

#include <stdio.h>
#include <math.h>

int main(void) {
    double principal, percentage_change, future_value;
    float interest_rate, term, compounding_period_rate, num_compounds;
    int compounds_per_year;

    printf("--- Compound Interest Calculator ---\n");
    printf("This calculator assumes values are in USD ($).\n");
    printf("Please enter all values without the currency symbol.\n\n");

    printf("Enter the principal:\n");
    scanf("%lf", &principal);

    printf("Enter the interest rate (APY, as a percentage):\n");
    scanf("%f", &interest_rate);

    printf("Enter the number of compounding periods per year (e.g. monthly = 12):\n");
    scanf("%d", &compounds_per_year);

    printf("Enter the investment term in years:\n");
    scanf("%f", &term);

    // Rate conversion
    interest_rate /= 100.0;

    // Formula calculations
    compounding_period_rate = 1.0 + (interest_rate / compounds_per_year);
    num_compounds = compounds_per_year * term;

    percentage_change = pow(compounding_period_rate, num_compounds);

    future_value = principal * percentage_change;

    printf("\nPrincipal: $%.2f\n", principal);
    printf("Interest rate (APY): %.2f%%\n", interest_rate * 100);
    printf("Compounding periods per year: %d\n", compounds_per_year);

    // Rudimentary printing logic
    if ((term - 1) <= .001) {
        printf("Investment term: %.1f year\n", term);  
    } else {
        printf("Investment term: %.1f years\n", term);
    }

    printf("Future value: $%.2f\n", future_value);

    return 0;
}