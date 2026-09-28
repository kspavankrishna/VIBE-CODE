with Ada.Real_Time; use Ada.Real_Time;
with Interfaces;    use Interfaces;

package body MCP_Tool_Budget_Arbiter is

   protected body Arbiter is

      ------------
      -- Fold --
      ------------

      --  Folds one ledger event into the running hash chain using FNV-1a
      --  style mixing. This is a fast, dependency free integrity check,
      --  not a cryptographic signature: it will catch corruption,
      --  truncation or reordering of the retained ledger window, but it
      --  is not a defence against someone who can recompute the chain
      --  themselves. See the README for what "tamper evident" means here.
      function Fold
        (Previous : Unsigned_64;
         Event    : Ledger_Event_Kind;
         Tenant   : Tenant_Id;
         Amount   : Budget_Units;
         Sequence : Natural) return Unsigned_64
      is
         FNV_Prime : constant Unsigned_64 := 16#0000_0100_0000_01B3#;
         Hash      : Unsigned_64 := Previous;

         procedure Mix (Value : Unsigned_64) is
         begin
            Hash := (Hash xor Value) * FNV_Prime;
         end Mix;
      begin
         Mix (Unsigned_64 (Ledger_Event_Kind'Pos (Event)));
         Mix (Unsigned_64 (Sequence));
         Mix (Unsigned_64 (Amount));

         declare
            Plain : constant String := Tenant_Strings.To_String (Tenant);
         begin
            for C of Plain loop
               Mix (Unsigned_64 (Character'Pos (C)));
            end loop;
         end;

         return Hash;
      end Fold;

      -------------------
      -- Append_Ledger --
      -------------------

      procedure Append_Ledger
        (Event  : Ledger_Event_Kind;
         Tenant : Tenant_Id;
         Amount : Budget_Units)
      is
         Slot      : constant Natural := Next_Sequence mod Max_Ledger_Entries;
         New_Chain : constant Unsigned_64 :=
           Fold (Chain_Hash, Event, Tenant, Amount, Next_Sequence);
      begin
         Ledger (Slot) :=
           (Sequence => Next_Sequence,
            Event    => Event,
            Tenant   => Tenant,
            Amount   => Amount,
            Chain    => New_Chain);

         Chain_Hash    := New_Chain;
         Next_Sequence := Next_Sequence + 1;

         if Ledger_Count < Max_Ledger_Entries then
            Ledger_Count := Ledger_Count + 1;
         end if;
      end Append_Ledger;

      -------------
      -- Reserve --
      -------------

      procedure Reserve
        (Tenant : Tenant_Id;
         Amount : Budget_Units;
         Outcome : out Reservation_Result)
      is
      begin
         if Amount = 0 then
            Outcome := (Status => Rejected_Invalid_Amount, Id => No_Reservation);
            return;
         end if;

         if Amount > Balance then
            Outcome :=
              (Status => Rejected_Insufficient_Budget, Id => No_Reservation);
            return;
         end if;

         for I in Slots'Range loop
            if not Slots (I).In_Use then
               Slots (I) :=
                 (In_Use     => True,
                  Tenant     => Tenant,
                  Amount     => Amount,
                  Issued_At  => Clock,
                  Expires_At => Clock + Lease);

               Balance := Balance - Amount;
               Append_Ledger (Reserved, Tenant, Amount);

               Outcome := (Status => Granted, Id => I);
               return;
            end if;
         end loop;

         Outcome := (Status => Rejected_Table_Full, Id => No_Reservation);
      end Reserve;

      ------------
      -- Commit --
      ------------

      procedure Commit
        (Id          : Reservation_Id;
         Actual_Cost : Budget_Units;
         Ok          : out Boolean)
      is
      begin
         if Id = No_Reservation or else not Slots (Id).In_Use then
            Ok := False;
            return;
         end if;

         declare
            Held   : constant Budget_Units := Slots (Id).Amount;
            Payer  : constant Tenant_Id := Slots (Id).Tenant;
            Charge : constant Budget_Units :=
              (if Actual_Cost > Held then Held else Actual_Cost);
         begin
            Balance := Balance + (Held - Charge);
            Append_Ledger (Committed, Payer, Charge);
            Slots (Id).In_Use := False;
         end;

         Ok := True;
      end Commit;

      --------------
      -- Rollback --
      --------------

      procedure Rollback
        (Id : Reservation_Id;
         Ok : out Boolean)
      is
      begin
         if Id = No_Reservation or else not Slots (Id).In_Use then
            Ok := False;
            return;
         end if;

         Balance := Balance + Slots (Id).Amount;
         Append_Ledger (Rolled_Back, Slots (Id).Tenant, Slots (Id).Amount);
         Slots (Id).In_Use := False;

         Ok := True;
      end Rollback;

      ------------------
      -- Expire_Stale --
      ------------------

      procedure Expire_Stale
        (Now           : Ada.Real_Time.Time;
         Expired_Count : out Natural)
      is
      begin
         Expired_Count := 0;

         for I in Slots'Range loop
            if Slots (I).In_Use and then Now > Slots (I).Expires_At then
               Balance := Balance + Slots (I).Amount;
               Append_Ledger (Expired, Slots (I).Tenant, Slots (I).Amount);
               Slots (I).In_Use := False;
               Expired_Count := Expired_Count + 1;
            end if;
         end loop;
      end Expire_Stale;

      ---------------
      -- Available --
      ---------------

      function Available return Budget_Units is
      begin
         return Balance;
      end Available;

      -----------------------
      -- Outstanding_Count --
      -----------------------

      function Outstanding_Count return Natural is
         Count : Natural := 0;
      begin
         for I in Slots'Range loop
            if Slots (I).In_Use then
               Count := Count + 1;
            end if;
         end loop;
         return Count;
      end Outstanding_Count;

      -----------------------------
      -- Verify_Ledger_Integrity --
      -----------------------------

      function Verify_Ledger_Integrity return Boolean is
         Oldest_Seq : Natural;
         Running    : Unsigned_64;
      begin
         if Ledger_Count = 0 then
            return True;
         end if;

         if Ledger_Count < Max_Ledger_Entries then
            Oldest_Seq := 0;
            Running    := Initial_Chain_Seed;
         else
            Oldest_Seq := Next_Sequence - Max_Ledger_Entries;
            Running    := Ledger (Oldest_Seq mod Max_Ledger_Entries).Chain;
         end if;

         --  When the buffer has wrapped, the oldest retained record's own
         --  preimage is gone, so it is trusted as the starting anchor and
         --  verification walks forward from the record after it. When it
         --  has never wrapped, sequence 0 is verified against the known
         --  seed, same as everything after it.
         for Seq in (if Oldest_Seq = 0 then 0 else Oldest_Seq + 1) ..
                    Next_Sequence - 1
         loop
            declare
               Rec      : Ledger_Record renames Ledger (Seq mod Max_Ledger_Entries);
               Expected : constant Unsigned_64 :=
                 Fold (Running, Rec.Event, Rec.Tenant, Rec.Amount, Rec.Sequence);
            begin
               if Rec.Sequence /= Seq or else Rec.Chain /= Expected then
                  return False;
               end if;
               Running := Rec.Chain;
            end;
         end loop;

         return True;
      end Verify_Ledger_Integrity;

      -----------------
      -- Copy_Ledger --
      -----------------

      procedure Copy_Ledger
        (Into  : out Ledger_Window;
         Count : out Natural)
      is
         Oldest_Seq : Natural;
      begin
         Count := Ledger_Count;

         if Ledger_Count < Max_Ledger_Entries then
            Oldest_Seq := 0;
         else
            Oldest_Seq := Next_Sequence - Max_Ledger_Entries;
         end if;

         for J in 1 .. Ledger_Count loop
            Into (Into'First + J - 1) :=
              Ledger ((Oldest_Seq + J - 1) mod Max_Ledger_Entries);
         end loop;
      end Copy_Ledger;

   end Arbiter;

end MCP_Tool_Budget_Arbiter;
