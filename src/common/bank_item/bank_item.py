class BankItem:
    def __init__(self, bank_name: str, bank_id: int):
        self.bank_name = bank_name
        self.bank_id = bank_id

    def __eq__(self, other):
        return self.bank_id == other.bank_id

    def __lt__(self, other):
        return self.bank_id < other.bank_id

    def __str__(self):
        return f"bank_name: {self.bank_name}, bank_id: {self.bank_id}"
