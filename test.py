#!/usr/bin/env python


def main():
    push_list = []
    v_in_address = ['82aVBktFYD5XR4BX2nuvUSGvXPMxHteZgiCVjqyfTssPHkF9VmDm2YCDGCSAPr4dXy5N5EoHsVFjf2eBe3vncys35u3iMKP', '82b8V9sSuPkZLGT1m6pyjE3rbeUAad9g9HC1DLupTLnreerP5mEb3DwTP2tA7FDTXCD6HvxR4JS8rC7GAbXrqLuPELD15pn']
    v_out_infos = [{'Txid': '067851047413416ad456b9eb640762ca4236a2b934eea81d2cb69e6da25a7671', 'Type': 'XMR', 'From': '', 'To': '44DE9vqmRy1DouwGArQGe7depyt5Ap6DMEL8kGTHtTQCjGEER1yByFL7g6FUzP9pyfdALSHWLJCMhMUZEwP6sgo1BRbQa2A', 'Amount': 0, 'Time': 1582273177, 'BlockNumber': 2038312, 'Contract': '', 'Charge': False, 'Memo': '', 'Fee': '100000000', 'Action': '', 'Valid': True, 'VoutsIndex': 1}]
    v_in_length = len(v_in_address)
    v_out_length = len(v_out_infos)
    v_out_sort_infos = sorted(v_out_infos, key=lambda x: x["VoutsIndex"])

    max_len = max(v_out_length, v_in_length)
    for num in range(max_len):
        v_in_idx = num if num < v_in_length else -1
        v_out_idx = num if num < v_out_length else -1
        tx_tp = v_out_sort_infos[v_out_idx] if v_out_length - 1 >= v_out_idx else v_out_sort_infos[-1]
        tmp = tx_tp.copy()
        tmp['From'] = v_in_address[v_in_idx] if v_in_address else ''
        # print(10*"*", tmp, id(tmp))
        push_list.append(tmp)

    print(push_list)


if __name__ == "__main__":
    main()
